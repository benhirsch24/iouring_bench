use io_uring::{IoUring, opcode, squeue::Entry, types};

use std::collections::HashMap;
use std::os::fd::AsRawFd;

use log::{debug, error, info};

// only support reads of 1KB
static BUFFER_SIZE : usize = 1024*1024;

struct Request {
    fd: types::Fd,
    buffer: Vec<u8>,
    filled: usize,
    response: Option<String>,
    responded: bool,
}

const ACCEPT_CODE: u64 = opcode::Accept::CODE as u64;

impl Request {
    fn new(fd: types::Fd) -> Request {
        Request {
            fd,
            buffer: vec![0u8; BUFFER_SIZE],
            filled: 0,
            response: None,
            responded: false,
        }
    }

    unsafe fn read(&mut self) -> Entry {
        let ptr = unsafe { self.buffer.as_mut_ptr().add(self.filled) };
        let read_e = opcode::Recv::new(self.fd, ptr, (BUFFER_SIZE - self.filled) as u32);
        return read_e.build().user_data(self.fd.0 as u64).into();
    }

    fn advance(&mut self, n: usize) {
        self.filled += n;
    }

    unsafe fn respond(&mut self, resp: &str) -> Entry {
        self.response = Some(resp.to_string());
        let ptr = self.response.as_mut().unwrap().as_mut_ptr();
        let send_e = opcode::Send::new(self.fd, ptr, self.response.as_ref().unwrap().len() as u32);
        return send_e.build().user_data(self.fd.0 as u64).into();
    }
}

fn main() -> Result<(), std::io::Error> {
    env_logger::init();
    let mut uring = IoUring::new(32).expect("io_uring");

    let listener = std::net::TcpListener::bind("127.0.0.1:80").expect("tcp listener");
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
                        info!("Accept! flags: {} result: {} ud: {}", e.flags(), e.result(), e.user_data());

                        // Create a new request object around this file descriptor and enqueue the
                        // first read
                        let mut req = Request::new(types::Fd(e.result()));
                        let re = unsafe { req.read() };
                        to_submit.push(re);
                        reqs.insert(e.result(), req);
                    },
                    _ => {
                        // Check if this is in our requests hashmap
                        let fd = e.user_data() as i32;
                        let mut remove = false;
                        {
                            let req = match reqs.get_mut(&fd) {
                                Some(r) => r,
                                None => {
                                    info!("No outstanding request for flags: {} result: {} ud: {}", e.flags(), e.result(), e.user_data());
                                    continue;
                                },
                            };

                            // Get the request out of the request map from the user data on the
                            // completion entry
                            info!("Recv! flags: {} result: {} ud: {}", e.flags(), e.result(), e.user_data());
                            if e.result() == -1 {
                                error!("Request error: {}", req.fd.0);
                                unsafe { libc::close(req.fd.0); };
                                remove = true;
                            } else if e.result() == 0 {
                                info!("Request done {}", std::str::from_utf8(&req.buffer).unwrap());
                                unsafe { libc::close(req.fd.0); };
                                remove = true;
                            } else {
                                if req.responded {
                                    info!("Responded! {}", fd);
                                    unsafe { libc::close(req.fd.0); };
                                    remove = true;
                                } else {
                                    // Advance the request internal offset pointer by how many bytes were read
                                    // (the result value of the read call)
                                    req.advance(e.result() as usize);
                                    let e = unsafe { req.read() };
                                    to_submit.push(e);

                                    // Try to parse the request
                                    let mut headers = [httparse::EMPTY_HEADER; 16];
                                    let mut r = httparse::Request::new(headers.as_mut_slice());
                                    let res = r.parse(&req.buffer[..req.filled]).expect("parse error");
                                    if res.is_complete() {
                                        let send = unsafe { req.respond("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok") };
                                        to_submit.push(send);
                                        req.responded = true;
                                        debug!("Enqueued response {}", fd);
                                    }
                                }
                            }
                            if remove {
                                reqs.remove(&fd);
                            }
                        }

                    },
                }
            }
            for e in to_submit {
                unsafe {
                    uring.submission().push(&e).expect("push read");
                }
            }
        }
    }
    Ok(())
}
