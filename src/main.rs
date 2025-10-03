use bytes::Bytes;
use clap::{Parser};
use io_uring::{IoUring, opcode, squeue::Entry, types};

use std::collections::HashMap;
use std::os::fd::AsRawFd;
use std::rc::Rc;

use log::{debug, error, info, trace, warn};

static BUFFER_SIZE : usize = 1024*1024;

const ACCEPT_CODE: u64 = opcode::Accept::CODE as u64;
const TIMEOUT_CODE: u64 = opcode::Timeout::CODE as u64;
const NOT_FOUND: &'static str = "HTTP/1.1 404 Not Found\r\nContent-Length: 8\r\n\r\nNotFound";
const HEALTH_OK: &'static str = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
const SILLY_TEXT: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/assets/silly_text.txt"));

struct Request {
    fd: types::Fd,
    buffer: Vec<u8>,
    filled: usize,
    response: Option<Bytes>,
    responded: bool,
    cache: Rc<HashMap<String, String>>,
    sent: usize,
    chunk_size: usize,
}

impl Request {
    fn new(fd: types::Fd, cache: Rc<HashMap<String, String>>, chunk_size: usize) -> Request {
        Request {
            fd,
            buffer: vec![0u8; BUFFER_SIZE],
            filled: 0,
            response: None,
            responded: false,
            cache,
            sent: 0,
            chunk_size,
        }
    }

    unsafe fn read(&mut self) -> Entry {
        let ptr = unsafe { self.buffer.as_mut_ptr().add(self.filled) };
        let read_e = opcode::Recv::new(self.fd, ptr, (BUFFER_SIZE - self.filled) as u32);
        return read_e.build().user_data(self.fd.0 as u64).into();
    }

    fn advance_read(&mut self, n: usize) {
        self.filled += n;
    }

    fn advance_write(&mut self, n: usize) {
        self.sent += n;
    }

    fn done(&self) -> bool {
        self.sent == self.response.as_ref().unwrap().len()
    }

    fn set_response(&mut self, resp: &str) {
        // Store response in this request as it needs to be allocated until the kernel has sent it
        // Eventually this could be a pointer to kernel owned buffer
        self.response = Some(Bytes::copy_from_slice(resp.as_bytes()));
    }

    unsafe fn send(&mut self) -> Entry {
        let len = self.response.as_ref().unwrap().len();

        let start = self.sent;
        let end = if start + self.chunk_size > len { len } else { start + self.chunk_size };
        let ptr = self.response.as_ref().unwrap().slice(start..end).as_ptr();
        let to_send : u32 = (end - start) as u32;
        let send_e = opcode::Send::new(self.fd, ptr, to_send);
        return send_e.build().user_data(self.fd.0 as u64).into();
    }

    fn serve(&mut self) -> Vec<Entry> {
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut r = httparse::Request::new(headers.as_mut_slice());
        let request = r.parse(&self.buffer[..self.filled]).expect("parse error");
        let mut sqe = Vec::new();

        // Not finished with request, keep reading
        if !request.is_complete() {
            let e = unsafe { self.read() };
            sqe.push(e);
            return sqe;
        }

        // We have a request, let's route
        match r.path {
            Some(path) => {
                match path {
                    p if p.starts_with("/object") => {
                        let parts = p.split("/").collect::<Vec<_>>();
                        if parts.len() != 3 {
                            self.set_response(&format!("HTTP/1.1 400 Bad Request\r\nContent-Length: 25\r\n\r\nExpected /object/<object>"));
                            let send = unsafe { self.send() };
                            sqe.push(send);
                        } else {
                            if let Some(o) = self.cache.get(&parts[2].to_string()) {
                                self.set_response(&format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}", o.len(), o));
                                let send = unsafe { self.send() };
                                sqe.push(send);
                            } else {
                                self.set_response(NOT_FOUND);
                                let send = unsafe { self.send() };
                                sqe.push(send);
                            }
                        }
                    },
                    "/health" => {
                        self.set_response(HEALTH_OK);
                        let send = unsafe { self.send() };
                        sqe.push(send);
                    },
                    _ => {
                        self.set_response(NOT_FOUND);
                        let send = unsafe { self.send() };
                        sqe.push(send);
                    }
                }
            },
            None => {
                self.set_response(NOT_FOUND);
                let send = unsafe { self.send() };
                sqe.push(send);
            }
        }

        self.responded = true;
        sqe
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Chunk size to send the file in chunks of
    #[arg(short, long, default_value_t = 4096)]
    chunk_size: usize,

    /// Size of uring submission queue
    #[arg(short, long, default_value_t = 1024)]
    uring_size: u32,

    /// Number of submissions in the backlog before submitting to uring
    #[arg(short, long, default_value_t = 64)]
    submissions_threshold: usize,

    /// Interval for kernel submission queue polling. If 0 then sqpoll is disabled. Default 0.
    #[arg(short = 'i', long, default_value_t = 0)]
    sqpoll_interval_ms: u32,
}

fn main() -> Result<(), std::io::Error> {
    env_logger::init();
    let args = Args::parse();
    info!("Using chunk size {}", args.chunk_size);
    let mut uring: IoUring<io_uring::squeue::Entry, io_uring::cqueue::Entry> = if args.sqpoll_interval_ms > 0 {
        IoUring::builder().setup_sqpoll(args.sqpoll_interval_ms).build(args.uring_size).expect("uring")
    } else {
        IoUring::builder().build(args.uring_size).expect("uring")
    };
    let uring_usize : usize = args.uring_size.try_into().unwrap();

    // Here's our super simple statically allocated cache
    let mut cache = Rc::new(HashMap::<String, String>::new());
    Rc::get_mut(&mut cache).unwrap().insert("1".to_string(), SILLY_TEXT.to_string());

    let listener = std::net::TcpListener::bind("0.0.0.0:8080").expect("tcp listener");
    let listener_fd = listener.as_raw_fd();
    let lfd = types::Fd(listener_fd);
    let mut sockaddr: libc::sockaddr = unsafe { std::mem::zeroed() };
    let mut addrlen: libc::socklen_t = std::mem::size_of::<libc::sockaddr>() as _;

    // Map<Fd, Request object>
    let mut reqs = HashMap::new();
    let mut to_submit = vec![];
    let mut submitted_last_period = 0;
    let mut completions_last_period = 0;
    let mut accept_inflight = 0;
    let mut timeout_inflight = false;
    let mut submit_and_wait = 0;
    let ts = types::Timespec::new().sec(10).nsec(0);
    let mut last_submit = std::time::Instant::now();
    loop {
        let mut submitted = 0;
        let mut completed = 0;

        let submission_available = {
            let s = uring.submission();
            s.capacity() - s.len()
        };
        if submission_available <= uring_usize && !timeout_inflight {
            trace!("Submitting timeout");
            let timeout = opcode::Timeout::new(&ts as *const types::Timespec)
                .count(0)
                .build()
                .user_data(TIMEOUT_CODE);
            unsafe { uring.submission().push(&timeout).expect("timeout") };
            timeout_inflight = true;
            submitted += 1;
        }

        // Keep ~8 accepts in flight at all times
        // TODO: Multishot accept
        let accept_gap = 8 - accept_inflight;
        let submission_available = {
            let s = uring.submission();
            s.capacity() - s.len()
        };
        if submission_available >= accept_gap {
            trace!("Submitting {} accepts avail={submission_available}", accept_gap);
            for _ in 0..accept_gap {
                accept_inflight += 1;
                let accept_e = opcode::Accept::new(lfd, &mut sockaddr, &mut addrlen).build().user_data(ACCEPT_CODE);
                unsafe { uring.submission().push(&accept_e).expect("push accept") };
            }
        }

        // If there are events to submit add them to the submission queue up to the uring
        // submission queue size.
        let submission_available = {
            let s = uring.submission();
            s.capacity() - s.len()
        };
        if to_submit.len() > 0 && submission_available > 0 {
            let num_to_submit = std::cmp::min(to_submit.len(), submission_available);
            for _ in 0..num_to_submit {
                let e = to_submit.pop().unwrap();
                unsafe {
                    // TODO: Add backlog if the submission queue is full.
                    uring.submission().push(&e).expect("push");
                }
                submitted_last_period += 1;
                submitted += 1;
            }
        }

        // Check for completions
        uring.completion().sync();
        for e in uring.completion() {
            completions_last_period += 1;
            completed += 1;
            match e.user_data() {
                TIMEOUT_CODE => {
                    if e.result() < 0 {
                        warn!("Timeout result <0 {}", e.result());
                    }
                    timeout_inflight = false;
                    info!("Metrics: submitted_last_period={submitted_last_period} completions_last_period={completions_last_period} submit_and_wait={submit_and_wait} backlog={}", to_submit.len());
                    submitted_last_period = 0;
                    completions_last_period = 0;
                    submit_and_wait = 0;
                },
                ACCEPT_CODE => {
                    accept_inflight -= 1;
                    if e.result() == 127 {
                        error!("Failed to accept");
                        continue;
                    }
                    trace!("Accept! flags: {} result: {} ud: {}", e.flags(), e.result(), e.user_data());

                    // Create a new request object around this file descriptor and enqueue the
                    // first read
                    let mut req = Request::new(types::Fd(e.result()), cache.clone(), args.chunk_size);
                    let re = unsafe { req.read() };
                    to_submit.push(re);
                    reqs.insert(e.result(), req);
                },
                _ => {
                    // Get the request out of our outstanding requests hashmap
                    let fd = e.user_data() as i32;
                    let req = match reqs.get_mut(&fd) {
                        Some(r) => r,
                        None => {
                            warn!("No outstanding request for flags: {} result: {} ud: {}", e.flags(), e.result(), e.user_data());
                            continue;
                        },
                    };

                    if e.result() == -1 {
                        error!("Request error: {}", req.fd.0);
                        unsafe { libc::close(req.fd.0); };
                        reqs.remove(&fd);
                        continue;
                    }
                    if e.result() == 0 {
                        unsafe { libc::close(req.fd.0); };
                        reqs.remove(&fd);
                        continue;
                    }
                    if e.result() < 0 {
                        let error = match -e.result() {
                            libc::EFAULT => "efault",
                            libc::EPIPE => "epipe",
                            libc::EIO => "eio",
                            libc::EINVAL => "einval",
                            libc::EBADF => "ebadf",
                            104 => "connection reset by peer",
                            _ => "other",
                        };
                        error!("Error on fd: {} {} {} {}", fd, e.result(), error, if req.responded { "responded" } else { "started" });
                        unsafe { libc::close(req.fd.0); };
                        reqs.remove(&fd);
                        continue;
                    }

                    // The completion queue returned a successful write response
                    if req.responded {
                        trace!("Write event! flags: {} result: {} ud: {}", e.flags(), e.result(), e.user_data());
                        req.advance_write(e.result().try_into().unwrap());
                        // TODO: No keep alive implemented yet
                        if req.done() {
                            unsafe { libc::close(req.fd.0); };
                            reqs.remove(&fd);
                        } else {
                            let send_e = unsafe { req.send() };
                            to_submit.push(send_e);
                        }
                        continue;
                    }
                    trace!("Read event! flags: {} result: {} ud: {}", e.flags(), e.result(), e.user_data());

                    // Advance the request internal offset pointer by how many bytes were read
                    // (the result value of the read call)
                    req.advance_read(e.result() as usize);

                    // Parse the request and submit any calls to the submission queue
                    let entries = req.serve();
                    for e in entries {
                        to_submit.push(e);
                    }
                },
            }
        }

        // If there are events in the submission queue call the non-blocking submit method.
        if uring.submission().len() > args.submissions_threshold {
            let n = uring.submit().unwrap();
            last_submit = std::time::Instant::now();
            debug!("Submit submitted={n}");
        }

        trace!("Metrics: submitted={submitted} completed={completed}");

        // If the submission queue is empty and the completion queue is empty, then submit and wait
        // for something to happen
        if (to_submit.len() == 0 && completed == 0) || (args.sqpoll_interval_ms > 0 && last_submit.elapsed().as_millis() > args.sqpoll_interval_ms.into()) {
            submit_and_wait += 1;
            last_submit = std::time::Instant::now();
            uring.submit_and_wait(1)?;
        }
    }
    Ok(())
}
