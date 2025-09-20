use io_uring::{IoUring, opcode, squeue::Entry, types};

use std::collections::HashMap;
use std::os::fd::AsRawFd;

use log::info;

// only support reads of 1KB
static BUFFER_SIZE : usize = 1024*1024;

struct Request {
    fd: types::Fd,
    buffer: Vec<u8>,
    filled: usize,
}

const ACCEPT_CODE: u64 = opcode::Accept::CODE as u64;

impl Request {
    fn new(fd: types::Fd) -> Request {
        Request {
            fd,
            buffer: vec![0u8; BUFFER_SIZE],
            filled: 0,
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
    let mut conns = HashMap::new();
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
                        let mut req = Request::new(types::Fd(e.result()));
                        let re = unsafe { req.read() };
                        to_submit.push(re);
                        conns.insert(e.result(), req);
                    },
                    _ => {
                        // Check if this is in our requests hashmap
                        if !conns.contains_key(&(e.user_data() as i32)) {
                            info!("No outstanding request for flags: {} result: {} ud: {}", e.flags(), e.result(), e.user_data());
                        }
                        info!("Recv! flags: {} result: {} ud: {}", e.flags(), e.result(), e.user_data());
                        let req = conns.get_mut(&(e.user_data() as i32)).expect("Should have fd");
                        if e.result() == -1 {
                            panic!("Recv error");
                        } else if e.result() == 0 {
                            info!("Request done {}", std::str::from_utf8(&req.buffer).unwrap());
                            unsafe { libc::close(req.fd.0); };
                            conns.remove(&(e.user_data() as i32));
                        } else {
                            req.advance(e.result() as usize);
                            let e = unsafe { req.read() };
                            to_submit.push(e);
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
