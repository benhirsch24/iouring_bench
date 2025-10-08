use bytes::Bytes;
use clap::{Parser};
use io_uring::{IoUring, opcode, squeue::Entry, types};
use histogram::Histogram;
use log::{debug, error, info, trace, warn};

use std::cell::RefCell;
use std::collections::HashMap;
use std::os::fd::AsRawFd;
use std::rc::Rc;

pub mod user_data;
use user_data::{Op, UserData};

static BUFFER_SIZE : usize = 1024*1024;

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
    write_timing_histogram: Rc<RefCell<Histogram>>,
    last_write: Option<std::time::Instant>,
}

impl Request {
    fn new(fd: types::Fd, cache: Rc<HashMap<String, String>>, write_timing_histogram: Rc<RefCell<Histogram>>, chunk_size: usize) -> Request {
        Request {
            fd,
            buffer: vec![0u8; BUFFER_SIZE],
            filled: 0,
            response: None,
            responded: false,
            cache,
            sent: 0,
            chunk_size,
            write_timing_histogram,
            last_write: None,
        }
    }

    unsafe fn read(&mut self) -> Entry {
        let ptr = unsafe { self.buffer.as_mut_ptr().add(self.filled) };
        let read_e = opcode::Recv::new(self.fd, ptr, (BUFFER_SIZE - self.filled) as u32);
        let ud = UserData::new(Op::Recv, self.fd.0);
        return read_e.build().user_data(ud.into_u64()).into();
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
        let ud = UserData::new(Op::Send, self.fd.0);

        // Stats on time between writes
        if let Some(n) = self.last_write {
            self.write_timing_histogram.borrow_mut().increment(n.elapsed().as_micros() as u64).expect("increment");
        }
        self.last_write = Some(std::time::Instant::now());

        return send_e.build().user_data(ud.into_u64()).into();
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
    #[arg(short, long, default_value_t = 4096)]
    uring_size: u32,

    /// Number of submissions in the backlog before submitting to uring
    #[arg(short, long, default_value_t = 1024)]
    submissions_threshold: usize,

    /// Interval for kernel submission queue polling. If 0 then sqpoll is disabled. Default 0.
    #[arg(short = 'i', long, default_value_t = 0)]
    sqpoll_interval_ms: u32,
}

fn main() -> Result<(), std::io::Error> {
    env_logger::init();
    let args = Args::parse();
    info!("{args:?}");
    let mut uring: IoUring<io_uring::squeue::Entry, io_uring::cqueue::Entry> = if args.sqpoll_interval_ms > 0 {
        IoUring::builder()
            .setup_cqsize(args.uring_size*2)
            .setup_sqpoll(args.sqpoll_interval_ms)
            .build(args.uring_size).expect("uring")
    } else {
        IoUring::builder()
            .setup_cqsize(args.uring_size*2)
            .build(args.uring_size).expect("uring")
    };

    // Here's our super simple statically allocated cache
    let mut cache = Rc::new(HashMap::<String, String>::new());
    Rc::get_mut(&mut cache).unwrap().insert("1".to_string(), SILLY_TEXT.to_string());
    let write_timing_histogram = Rc::new(RefCell::new(Histogram::new(7, 64).expect("histogram")));
    let mut submit_and_wait_batch_size = Histogram::new(7, 64).expect("sawbs histo");

    let listener = std::net::TcpListener::bind("0.0.0.0:8080").expect("tcp listener");
    let listener_fd = listener.as_raw_fd();
    let lfd = types::Fd(listener_fd);
    let mut reqs = HashMap::new();
    let mut to_submit = vec![];
    let mut submitted_last_period = 0;
    let mut completions_last_period = 0;
    let mut submit_and_wait = 0;
    let ts = types::Timespec::new().sec(5).nsec(0);
    let mut last_submit = std::time::Instant::now();

    // Arm multi-shot accept so we don't have to continually resubmit
    let accept_ud = UserData::new(Op::Accept, 0);
    let accept_e = opcode::AcceptMulti::new(lfd).build().user_data(accept_ud.into_u64());
    unsafe { uring.submission().push(&accept_e).expect("push accept") };
    // Add the first timeout
    let timeout_ud = UserData::new(Op::Timeout, 0);
    let timeout = opcode::Timeout::new(&ts as *const types::Timespec)
        .flags(io_uring::types::TimeoutFlags::MULTISHOT)
        .count(0)
        .build()
        .user_data(timeout_ud.into_u64());
    unsafe { uring.submission().push(&timeout).expect("timeout") };
    uring.submit().expect("First submit");
    debug!("Multishot accept armed ud={}", accept_ud.into_u64());

    loop {
        let mut completed = 0;

        // Check for completions
        uring.completion().sync();
        for e in uring.completion() {
            completions_last_period += 1;
            completed += 1;
            trace!("completion result={} ud={}", e.result(), e.user_data());
            let ud = UserData::try_from(e.user_data()).expect("failed userdata extract");
            let fd = ud.fd();
            match ud.op().expect("op") {
                Op::Timeout => {
                    if e.result() != -62 {
                        warn!("Timeout result not 62: {}", e.result());
                    }

                    info!("Metrics: submitted_last_period={submitted_last_period} completions_last_period={completions_last_period} submit_and_wait={submit_and_wait} backlog={}", to_submit.len());

                    info!("submit_and_wait batch sizes");
                    let percentiles = [0.0, 50.0, 90.0, 99.0, 99.9, 99.99, 100.0];
                    if let Some(ps) = submit_and_wait_batch_size.percentiles(&percentiles).expect("saw percentiles") {
                        for p in ps {
                            info!("p{} range={:?} count={}", p.0, p.1.range(), p.1.count());
                        }
                    }

                    let percentiles = [0.0, 50.0, 90.0, 99.0, 100.0];
                    if let Some(ps) = write_timing_histogram.borrow().percentiles(&percentiles).expect("percentiles") {
                        debug!("Median time betwen writes for a request={}", ps[1].1.count());
                    }

                    submitted_last_period = 0;
                    completions_last_period = 0;
                    submit_and_wait = 0;
                },
                Op::Accept => {
                    if e.result() == 127 {
                        error!("Failed to accept");
                        continue;
                    }
                    trace!("Accept! flags: {} result: {} ud: {}", e.flags(), e.result(), e.user_data());

                    // Create a new request object around this file descriptor and enqueue the
                    // first read
                    let mut req = Request::new(types::Fd(e.result()), cache.clone(), write_timing_histogram.clone(), args.chunk_size);
                    let re = unsafe { req.read() };
                    to_submit.push(re);
                    reqs.insert(e.result(), req);
                },
                _ => {
                    // Get the request out of our outstanding requests hashmap
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

        // Submit N batches of size threshold
        let num_batches = to_submit.len() / args.submissions_threshold;
        for _ in 0..num_batches {
            let batch: Vec<io_uring::squeue::Entry> = to_submit.drain(0..args.submissions_threshold).collect();
            unsafe { uring.submission().push_multiple(&batch) }.expect("push multiple");
            uring.submit().expect("Submitted");
            last_submit = std::time::Instant::now();
            submitted_last_period += 1;
        }

        // Submit and wait for a completion when:
        // 1. We're in SQPOLL mode and we haven't submitted in over the interval, so the kernel
        //    thread may have returned.
        // 2. We haven't submitted in some period of time because we haven't hit the batch threshold
        let should_sqpoll_submit = args.sqpoll_interval_ms > 0 && last_submit.elapsed().as_millis() > args.sqpoll_interval_ms.into();
        if should_sqpoll_submit || completed == 0 {
            let batch: Vec<io_uring::squeue::Entry> = to_submit.drain(..).collect();
            unsafe { uring.submission().push_multiple(&batch) }.expect("push multiple");
            debug!("Submit and wait last_submit={} backlog={} batch_size={} t_diff={}", last_submit.elapsed().as_millis(), to_submit.len(), batch.len(), last_submit.elapsed().as_micros());
            submit_and_wait_batch_size.increment(batch.len() as u64).unwrap();
            submit_and_wait += 1;
            last_submit = std::time::Instant::now();
            uring.submit_and_wait(1)?;
        }
    }
    Ok(())
}
