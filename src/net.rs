use io_uring::{opcode, types};
use log::trace;

use std::net::ToSocketAddrs;
use std::os::fd::{AsRawFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::uring;
use crate::callbacks::add_callback;
use crate::executor;

pub struct TcpListener {
    l: std::net::TcpListener,
    accept_multi_op: Option<u64>,
}

impl TcpListener {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> std::io::Result<TcpListener> {
        let l = std::net::TcpListener::bind(addr)?;
        Ok(TcpListener{
            l,
            accept_multi_op: None,
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

    pub fn accept_multi_fut(&mut self) -> AcceptFuture {
        if let Some(op_id) = self.accept_multi_op.as_ref() {
            return AcceptFuture { op_id: *op_id };
        }

        let fd = self.l.as_raw_fd();
        let opcode = opcode::AcceptMulti::new(types::Fd(fd));
        let op_id = executor::get_next_op_id();
        self.accept_multi_op = Some(op_id);
        executor::schedule_completion(op_id, true);
        uring::submit(opcode.build().user_data(op_id));
        AcceptFuture {
            op_id,
        }
    }
}

pub struct AcceptFuture {
    op_id: u64,
}

impl Future for AcceptFuture {
    type Output = std::io::Result<TcpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.as_ref();
        match executor::get_result(me.op_id) {
            Some(res) => {
                trace!("Got result {res}");
                Poll::Ready(Ok(TcpStream::new(res.into())))
            },
            None => {
                Poll::Pending
            }
        }
    }
}

pub struct TcpStream {
    fd: RawFd,
}

impl TcpStream {
    pub fn new(fd: RawFd) -> TcpStream {
        TcpStream {
            fd,
        }
    }

    pub fn as_raw_fd(&self) -> RawFd {
        self.fd
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
