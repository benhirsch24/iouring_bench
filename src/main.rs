use bytes::Bytes;
use io_uring::{IoUring, opcode, squeue::Entry, types};

use std::collections::HashMap;
use std::os::fd::AsRawFd;
use std::rc::Rc;

use log::{debug, error, info, warn};

// only support reads of 1KB
static BUFFER_SIZE : usize = 1024*1024;

const ACCEPT_CODE: u64 = opcode::Accept::CODE as u64;
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
}

impl Request {
    fn new(fd: types::Fd, cache: Rc<HashMap<String, String>>) -> Request {
        Request {
            fd,
            buffer: vec![0u8; BUFFER_SIZE],
            filled: 0,
            response: None,
            responded: false,
            cache,
            sent: 0,
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
        let byte_clone = (self.response.as_mut().unwrap()).clone();
        let slice = byte_clone.slice(self.sent..len);
        let ptr = slice.as_ptr();
        let to_send : u32 = (len - self.sent) as u32;
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

fn main() -> Result<(), std::io::Error> {
    env_logger::init();
    let mut uring = IoUring::new(32).expect("io_uring");

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
    loop {
        // Always accept
        let accept_e = opcode::Accept::new(lfd, &mut sockaddr, &mut addrlen);
        unsafe {
            uring.submission().push(&accept_e.build().user_data(ACCEPT_CODE).into()).expect("first push");
        }

        // Wait for something
        uring.submit_and_wait(1)?;

        // Check for completions
        {
            uring.completion().sync();
            let mut to_submit = vec![];
            for e in uring.completion() {
                match e.user_data() {
                    ACCEPT_CODE => {
                        if e.result() == 127 {
                            error!("Failed to accept");
                            continue;
                        }
                        info!("Accept! flags: {} result: {} ud: {}", e.flags(), e.result(), e.user_data());

                        // Create a new request object around this file descriptor and enqueue the
                        // first read
                        let mut req = Request::new(types::Fd(e.result()), cache.clone());
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

                        // The completion queue returned a successful write response
                        if req.responded {
                            debug!("Write event! flags: {} result: {} ud: {}", e.flags(), e.result(), e.user_data());
                            if e.result() < 0 {
                                let error = match -e.result() {
                                    libc::EFAULT => "efault",
                                    libc::EPIPE => "epipe",
                                    libc::EIO => "eio",
                                    libc::EINVAL => "einval",
                                    libc::EBADF => "ebadf",
                                    _ => "other",
                                };
                                error!("Error on write: {} {}", e.result(), error);
                                unsafe { libc::close(req.fd.0); };
                                reqs.remove(&fd);
                                continue;
                            }
                            req.advance_write(e.result().try_into().unwrap());
                            // TODO: This means no keep-alive
                            // TODO: Probably also want to check how many bytes we wrote in case we
                            // need another go-around
                            debug!("Responded! {} {}", fd, e.result());
                            if req.done() {
                                unsafe { libc::close(req.fd.0); };
                                reqs.remove(&fd);
                            } else {
                                let send_e = unsafe { req.send() };
                                to_submit.push(send_e);
                            }
                            continue;
                        }
                        debug!("Read event! flags: {} result: {} ud: {}", e.flags(), e.result(), e.user_data());

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
            // Actually submit to submission queue
            for e in to_submit {
                unsafe {
                    uring.submission().push(&e).expect("push read");
                }
            }
        }
    }
    Ok(())
}
