use io_uring::{opcode, types};

use std::net::ToSocketAddrs;
use std::os::fd::{AsRawFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::{cell::RefCell, rc::Rc};

use futures::{AsyncRead, io::{Error}};

use crate::uring;
use crate::callbacks::add_callback;
use crate::executor;

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

    pub fn accept_fut(&self) -> AcceptFuture {
        // TODO: Add persistent op id
        let op_id = executor::get_next_op_id();
        let fd = self.l.as_raw_fd();
        // TODO: Get current task
        let task_id = 0;
        let opcode = opcode::AcceptMulti::new(types::Fd(fd));
        executor::schedule_completion(task_id, op_id);
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
                println!("Got result {res}");
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
