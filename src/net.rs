use io_uring::{opcode, types};

use std::net::ToSocketAddrs;
use std::os::fd::{AsRawFd, RawFd};

use crate::uring;
use crate::callbacks::add_callback;

pub struct TcpListener {
    l: std::net::TcpListener,
}

impl TcpListener {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> std::io::Result<TcpListener> {
        let l = std::net::TcpListener::bind(addr)?;
        Ok(TcpListener{
            l,
        })
    }

    pub fn accept<F>(&self, f: F) -> anyhow::Result<()>
        where F: FnOnce(i32) -> anyhow::Result<()> + 'static
    {
        let fd = self.l.as_raw_fd();
        let op = opcode::AcceptMulti::new(types::Fd(fd));
        // TODO: Add callback that won't be removed
        let ud = add_callback(f);
        Ok(uring::submit(op.build().user_data(ud))?)
    }
}

#[derive(Copy, Clone)]
pub struct TcpStream {
    fd: RawFd,
}

impl TcpStream {
    pub fn new(fd: RawFd) -> TcpStream {
        TcpStream { fd }
    }

    pub fn recv<F>(&self, ptr: *mut u8, capacity: usize, f: F) -> anyhow::Result<()>
        where F: FnOnce(i32) -> anyhow::Result<()> + 'static
    {
        let op = opcode::Recv::new(types::Fd(self.fd), ptr, capacity as u32);
        let ud = add_callback(f);
        Ok(uring::submit(op.build().user_data(ud))?)
    }

    pub fn send<F>(&self, ptr: *const u8, len: usize, f: F) -> anyhow::Result<()>
        where F: FnOnce(i32) -> anyhow::Result<()> + 'static
    {
        let op = opcode::Send::new(types::Fd(self.fd), ptr, len as u32);
        let ud = add_callback(f);
        Ok(uring::submit(op.build().user_data(ud))?)
    }
}
