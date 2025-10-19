use io_uring::{opcode, types};

use std::net::ToSocketAddrs;
use std::os::fd::{AsRawFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::{cell::RefCell, rc::Rc};

use futures::{AsyncRead, io::{Error}};

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

struct SharedState {
    waker: Option<Waker>,
    result: Option<i32>,
}

impl SharedState {
    fn new() -> Self {
        Self {
            waker: None,
            result: None,
        }
    }
}

type RcRc<T> = Rc<RefCell<T>>;

struct AcceptFuture {
    listener: RawFd,
    operation_id: Option<u64>,
    shared_state: RcRc<SharedState>,
}

impl Future for AcceptFuture {
    type Output = std::io::Result<TcpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut me = Pin::into_inner(self);
        // If operation hasn't been submitted yet, submit it
        if me.operation_id.is_none() {
            me.operation_id = Some(1);
        }

        Poll::Pending
    }
}

pub struct TcpStream {
    fd: RawFd,
    shared_state: Rc<RefCell<SharedState>>,
}

impl TcpStream {
    pub fn new(fd: RawFd) -> TcpStream {
        TcpStream {
            fd,
            shared_state: Rc::new(RefCell::new(SharedState::new())),
        }
    }

    pub fn accept(&self) -> AcceptFuture {
        AcceptFuture {
            listener: self.fd,
            operation_id: None,
            shared_state: Rc::new(RefCell::new(SharedState::new())),
        }
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

//impl AsyncRead for TcpStream {
//    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, Error>> {
//        let me = Pin::into_inner(self);
//        let mut state = me.shared_state.borrow_mut();
//        if let Some(res) = state.result.take() {
//            return Poll::Ready(Ok(res as usize));
//        }
//        let fd = types::Fd(me.fd);
//        let op = opcode::Recv::new(fd, buf.as_mut_ptr(), buf.len() as u32);
//        let state = me.shared_state.clone();
//        let ud = add_callback(move |res| {
//            cx.waker().wake();
//            state.borrow_mut().result = Some(res as i32);
//            Ok(())
//        });
//        uring::submit(op.build().user_data(ud)).expect("poll read");
//        Poll::Pending
//    }
//}

//#[cfg(tests)]
//mod tests {
//    #[test]
//    fn read() {
//        // Create tcp listener
//        let listen = std::net::TcpListener::bind("127.0.0.1:8080").expect("tcp listener");
//        let handle = std::thread::spawn(move || {
//            let stream = listen.accept().expect("accept");
//            let stream = TcpStream::new(stream.as_raw_fd());
//            let start = std::time::Instant::now();
//        });
//    }
//}
