use io_uring::{IoUring, opcode, types};

use std::os::fd::AsRawFd;

fn main() {
    let mut uring = IoUring::new(32).expect("io_uring");

    let listener = std::net::TcpListener::bind("127.0.0.1:80").expect("tcp listener");
    let listener_fd = listener.as_raw_fd();
    let lfd = types::Fd(listener_fd);
    let mut sockaddr: libc::sockaddr = unsafe { std::mem::zeroed() };
    let mut addrlen: libc::socklen_t = std::mem::size_of::<libc::sockaddr>() as _;
    let accept_e = opcode::Accept::new(lfd, &mut sockaddr, &mut addrlen);
    unsafe {
        uring.submission().push(&accept_e.build().user_data(0x0e).into()).expect("first push");
    }

    loop {
        uring.submit_and_wait(1).expect("saw");
        break;
    }

    println!("Hello, world!");
}
