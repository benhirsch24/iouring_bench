use futures::{AsyncRead, AsyncWrite};
use io_uring::{opcode, types};
use log::{info, trace};
use socket2::{Socket, Domain, Type};

use std::io::Error;
use std::net::ToSocketAddrs;
use std::os::fd::{AsRawFd, IntoRawFd, RawFd};
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
        let l = net2::TcpBuilder::new_v4()?.reuse_address(true)?.bind(&addr)?.listen(1024)?;
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
        let ud = add_callback(f);
        Ok(uring::submit(op.build().user_data(ud))?)
    }

    pub fn accept_multi_fut(&mut self) -> std::io::Result<AcceptFuture> {
        if let Some(op_id) = self.accept_multi_op.as_ref() {
            return Ok(AcceptFuture { op_id: *op_id });
        }

        let fd = self.l.as_raw_fd();
        let opcode = opcode::AcceptMulti::new(types::Fd(fd));
        let op_id = executor::get_next_op_id();
        self.accept_multi_op = Some(op_id);
        executor::schedule_completion(op_id, true);
        trace!("Scheduling accept completion for op={op_id} fd={} task_id={}", fd, executor::get_task_id());
        if let Err(e) = uring::submit(opcode.build().user_data(op_id)) {
            Err(Error::new(std::io::ErrorKind::Other, format!("Uring problem: {e}")))
        } else {
            Ok(AcceptFuture {
                op_id,
            })
        }
    }
}

pub struct AcceptFuture {
    op_id: u64,
}

impl Future for AcceptFuture {
    type Output = std::io::Result<TcpStream>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.as_ref();
        match executor::get_result(me.op_id) {
            Some(res) => {
                trace!("Accepted result={res} op={}", me.op_id);
                if res < 0 {
                    Poll::Ready(Err(std::io::Error::from_raw_os_error(-res)))
                } else {
                    Poll::Ready(Ok(TcpStream::new(res.into())))
                }
            },
            None => {
                Poll::Pending
            }
        }
    }
}

pub struct ConnectFuture {
    op_id: u64,
    addr: Box<os_socketaddr::OsSocketAddr>,
    fd: RawFd,
}

impl Future for ConnectFuture {
    type Output = std::io::Result<TcpStream>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.as_ref();
        match executor::get_result(me.op_id) {
            Some(res) => {
                trace!("Got connect result {res} op={}", me.op_id);
                if res < 0 {
                    Poll::Ready(Err(std::io::Error::from_raw_os_error(-res)))
                } else {
                    Poll::Ready(Ok(TcpStream::new(me.fd)))
                }
            },
            None => {
                trace!("Connect pending task_id={}", executor::get_task_id());
                Poll::Pending
            }
        }
    }
}

pub struct TcpStream {
    fd: RawFd,
    read_op_id: Option<u64>,
    write_op_id: Option<u64>,
    close_op_id: Option<u64>,
}

impl TcpStream {
    pub fn new(fd: RawFd) -> TcpStream {
        TcpStream {
            fd,
            read_op_id: None,
            write_op_id: None,
            close_op_id: None,
        }
    }

    pub fn as_raw_fd(&self) -> RawFd {
        self.fd
    }

    pub fn connect<A: ToSocketAddrs>(addr: A) -> ConnectFuture {
        let op_id = executor::get_next_op_id();

        let socket_addr = addr.to_socket_addrs().ok().unwrap().next().unwrap();
        let os_socket_addr = Box::new(os_socketaddr::OsSocketAddr::from(socket_addr));
        let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(socket2::Protocol::TCP)).unwrap();

        let fd = socket.into_raw_fd();
        let ptr = os_socket_addr.as_ptr();
        let len = os_socket_addr.len();
        let opcode = opcode::Connect::new(types::Fd(fd), ptr, len);
        executor::schedule_completion(op_id, false);
        trace!("Scheduling connect completion for op={op_id} fd={} task_id={}", fd, executor::get_task_id());
        if let Err(e) = uring::submit(opcode.build().user_data(op_id)) {
            log::error!("Error submitting connect: {e}");
        }
        ConnectFuture { op_id, addr: os_socket_addr, fd }
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

impl AsyncRead for TcpStream {
    fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, futures::io::Error>> {
        let mut me = self.as_mut();

        // Only allow one read queued on a TcpStream at a time
        if me.read_op_id.is_some() {
            let op_id = me.read_op_id.unwrap();
            trace!("Polling {op_id}");
            return match executor::get_result(op_id) {
                Some(res) => {
                        me.read_op_id = None;
                    trace!("Got recv result {res} op_id={}", op_id);
                    if res < 0 {
                        Poll::Ready(Err(std::io::Error::from_raw_os_error(-res)))
                    } else {
                        trace!("Ready {op_id} {res}");
                        Poll::Ready(Ok(res as usize))
                    }
                },
                None => {
                    trace!("Read pending task_id={} op_id={op_id}", executor::get_task_id());
                    Poll::Pending
                }
            };
        }

        let op_id = executor::get_next_op_id();
        me.read_op_id = Some(op_id);
        let ptr = buf.as_mut_ptr();
        let capacity = buf.len() as u32;
        trace!("Scheduling recv completion for op={op_id} fd={} task_id={} len={capacity}", me.fd, executor::get_task_id());
        let op = opcode::Recv::new(types::Fd(me.fd), ptr, capacity);
        executor::schedule_completion(op_id, false);
        uring::submit(op.build().user_data(op_id)).expect("submit asyncread");

        Poll::Pending
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, futures::io::Error>> {
        let mut me = self.as_mut();

        // Only allow one write queued on a TcpStream at a time
        if me.write_op_id.is_some() {
            let op_id = me.write_op_id.unwrap();
            trace!("Polling {op_id}");
            return match executor::get_result(op_id) {
                Some(res) => {
                        me.write_op_id = None;
                    trace!("Got write result {res} fd={op_id}");
                    if res < 0 {
                        Poll::Ready(Err(std::io::Error::from_raw_os_error(-res)))
                    } else {
                        Poll::Ready(Ok(res as usize))
                    }
                },
                None => {
                    Poll::Pending
                }
            };
        }

        let op_id = executor::get_next_op_id();
        me.write_op_id = Some(op_id);
        let ptr = buf.as_ptr();
        let capacity = buf.len() as u32;
        let op = opcode::Send::new(types::Fd(me.fd), ptr, capacity);
        trace!("Scheduling send completion for op={op_id} fd={} task_id={} len={capacity}", me.fd, executor::get_task_id());
        executor::schedule_completion(op_id, false);
        if let Err(e) = uring::submit(op.build().user_data(op_id)) {
            Poll::Ready(Err(Error::new(std::io::ErrorKind::Other, format!("Uring problem: {e}"))))
        } else {
            Poll::Pending
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), futures::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), futures::io::Error>> {
        let mut me = self.as_mut();

        // Only allow one read queued on a TcpStream at a time
        if me.close_op_id.is_some() {
            let op_id = me.close_op_id.unwrap();
            return match executor::get_result(op_id) {
                Some(res) => {
                    if res < 0 {
                        Poll::Ready(Err(std::io::Error::from_raw_os_error(-res)))
                    } else {
                        Poll::Ready(Ok(()))
                    }
                },
                None => {
                    Poll::Pending
                }
            };
        }

        let op_id = executor::get_next_op_id();
        me.close_op_id = Some(op_id);
        let op = opcode::Close::new(types::Fd(me.fd));
        trace!("Scheduling close completion for op={op_id} fd={} task_id={}", me.fd, executor::get_task_id());
        executor::schedule_completion(op_id, false);
        uring::submit(op.build().user_data(op_id)).expect("submit close");

        Poll::Pending
    }
}
