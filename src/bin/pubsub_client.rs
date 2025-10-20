use io_uring::{opcode, types};
use log::{debug, error, info, trace, warn};
use bytes::{BufMut, Bytes, BytesMut};

use std::collections::HashMap;
use std::os::fd::{AsRawFd, RawFd};
use std::net::TcpStream;

use iouring_bench::uring;
use iouring_bench::user_data::{Op, UserData};

enum ConnectionResult {
    Continue,
    Done,
}

trait Connection {
    fn handle(&mut self, op: Op, res: i32) -> anyhow::Result<ConnectionResult>;
    fn shutdown(&self) -> anyhow::Result<()>;
}

#[derive(PartialEq)]
enum PublisherState {
    Init,
    SentHi,
    WaitOk,
    SentMsg1,
}

struct Publisher {
    conn: TcpStream,
    fd: RawFd,
    state: PublisherState,
    read_buffer: BytesMut,
    send_buffer: BytesMut,
}

impl Publisher {
    fn new() -> Publisher {
        let conn = TcpStream::connect("127.0.0.1:8080").expect("publish conn");
        let fd = conn.as_raw_fd();
        Publisher {
            conn: conn,
            fd,
            state: PublisherState::Init,
            read_buffer: BytesMut::with_capacity(1024),
            send_buffer: BytesMut::with_capacity(1024),
        }
    }

    fn read_line(&mut self) -> Option<Bytes> {
        for idx in 0..self.read_buffer.len() {
            if self.read_buffer[idx] == b'\n' && self.read_buffer[idx-1] == b'\r' {
                let buf = self.read_buffer.split_to(idx+1).freeze();
                return Some(buf);
            }
        }
        None
    }

    fn send_hi(&mut self) -> anyhow::Result<()> {
        if self.state != PublisherState::Init {
            anyhow::bail!("Publisher must be in state init to send hi");
        }
        self.send_buffer.put_slice(b"PUBLISH ch\r\n");
        let ptr = self.send_buffer.as_ptr();
        let to_send = self.send_buffer.len();
        let send_e = opcode::Send::new(types::Fd(self.fd), ptr, to_send.try_into().unwrap());
        let ud = UserData::new(Op::Send, self.fd);
        uring::submit(send_e.build().user_data(ud.into()))?;
        self.state = PublisherState::SentHi;
        Ok(())
    }

    fn send_ack(&mut self, n: usize) -> anyhow::Result<ConnectionResult> {
        match self.state {
            PublisherState::SentHi => {
                let ptr = self.read_buffer.as_mut_ptr();
                let read_e = opcode::Recv::new(types::Fd(self.fd), ptr, (self.read_buffer.capacity() - self.read_buffer.len()) as u32);
                let ud = UserData::new(Op::Recv, self.fd);
                let e = read_e.build().user_data(ud.into_u64()).into();
                uring::submit(e)?;
                self.state = PublisherState::WaitOk;
            },
            PublisherState::SentMsg1 => {
                let _ = self.send_buffer.split_to(n);
                if self.send_buffer.len() > 0 {
                    let ptr = self.send_buffer.as_ptr();
                    let to_send = self.send_buffer.len();
                    let send_e = opcode::Send::new(types::Fd(self.fd), ptr, to_send.try_into().unwrap());
                    let ud = UserData::new(Op::Send, self.fd);
                    uring::submit(send_e.build().user_data(ud.into()))?;
                }
                return Ok(ConnectionResult::Done);
            },
            _ => panic!("Unexpected publisher send_ack")
        }
        Ok(ConnectionResult::Continue)
    }

    fn recv(&mut self, res: usize) -> anyhow::Result<ConnectionResult> {
        match self.state {
            PublisherState::WaitOk => {
                unsafe { self.read_buffer.set_len(self.read_buffer.len()+res) };
                let line = self.read_line().expect("There should be one read by now");
                if line != "OK\r\n" {
                    anyhow::bail!("Expected OK");
                }
                let _ = self.send_buffer.split();
                self.send_buffer.put_slice(b"hello\r\n");
                let ptr = self.send_buffer.as_ptr();
                let to_send = self.send_buffer.len();
                let send_e = opcode::Send::new(types::Fd(self.fd), ptr, to_send.try_into().unwrap());
                let ud = UserData::new(Op::Send, self.fd);
                uring::submit(send_e.build().user_data(ud.into()))?;
                self.state = PublisherState::SentMsg1;
            }
            _ => panic!("Shouldn't have another read...")
        }
        Ok(ConnectionResult::Continue)
    }
}

impl Connection for Publisher {
    fn handle(&mut self, op: Op, res: i32) -> anyhow::Result<ConnectionResult> {
        match op {
            Op::Recv => {
                info!("Publisher Recv {res}");
                self.recv(res.try_into().unwrap())
            },
            Op::Send => {
                info!("Publisher Send {res}");
                self.send_ack(res.try_into().unwrap())
            },
            _ => panic!("unexpected op {op}")
        }
    }

    fn shutdown(&self) -> anyhow::Result<()> {
        info!("Shutting down publisher");
        Ok(self.conn.shutdown(std::net::Shutdown::Both)?)
    }
}

#[derive(PartialEq)]
enum SubscriberState {
    Init,
    SentHi,
    WaitOk,
    WaitMessage,
}

struct Subscriber {
    conn: TcpStream,
    fd: RawFd,
    state: SubscriberState,
    read_buffer: BytesMut,
    send_buffer: BytesMut,
}

impl Subscriber {
    fn new() -> Subscriber {
        let conn = TcpStream::connect("127.0.0.1:8080").expect("publish conn");
        let fd = conn.as_raw_fd();
        Subscriber {
            conn: conn,
            fd,
            state: SubscriberState::Init,
            read_buffer: BytesMut::with_capacity(1024),
            send_buffer: BytesMut::with_capacity(1024),
        }
    }

    fn read_line(&mut self) -> Option<Bytes> {
        for idx in 0..self.read_buffer.len() {
            if self.read_buffer[idx] == b'\n' && self.read_buffer[idx-1] == b'\r' {
                let buf = self.read_buffer.split_to(idx+1).freeze();
                return Some(buf);
            }
        }
        None
    }

    fn send_hi(&mut self) -> anyhow::Result<()> {
        if self.state != SubscriberState::Init {
            anyhow::bail!("Subscriber must be in state init to send hi");
        }
        self.send_buffer.put_slice(b"SUBSCRIBE ch\r\n");
        let ptr = self.send_buffer.as_ptr();
        let to_send = self.send_buffer.len();
        let send_e = opcode::Send::new(types::Fd(self.fd), ptr, to_send.try_into().unwrap());
        let ud = UserData::new(Op::Send, self.fd);
        uring::submit(send_e.build().user_data(ud.into()))?;
        self.state = SubscriberState::SentHi;
        Ok(())
    }

    fn send_ack(&mut self, _n: usize) -> anyhow::Result<ConnectionResult> {
        match self.state {
            SubscriberState::SentHi => {
                let ptr = self.read_buffer.as_mut_ptr();
                let read_e = opcode::Recv::new(types::Fd(self.fd), ptr, (self.read_buffer.capacity() - self.read_buffer.len()) as u32);
                let ud = UserData::new(Op::Recv, self.fd);
                let e = read_e.build().user_data(ud.into_u64()).into();
                uring::submit(e)?;
                self.state = SubscriberState::WaitOk;
            },
            _ => panic!("Unexpected subscriber send_ack")
        }
        Ok(ConnectionResult::Continue)
    }

    fn recv(&mut self, res: usize) -> anyhow::Result<ConnectionResult> {
        match self.state {
            SubscriberState::WaitOk => {
                unsafe { self.read_buffer.set_len(self.read_buffer.len()+res) };
                let line = self.read_line().expect("There should be one read by now");
                let msg = std::str::from_utf8(line.as_ref()).unwrap();
                info!("Wait ok {msg}");
                if line != "OK\r\n" {
                    anyhow::bail!("Expected OK");
                }

                // Enqueue another recv and wait for that
                let _ = self.read_buffer.split();
                let ptr = self.read_buffer.as_mut_ptr();
                let read_e = opcode::Recv::new(types::Fd(self.fd), ptr, (self.read_buffer.capacity() - self.read_buffer.len()) as u32);
                let ud = UserData::new(Op::Recv, self.fd);
                let e = read_e.build().user_data(ud.into_u64()).into();
                uring::submit(e)?;
                self.state = SubscriberState::WaitMessage;
            },
            SubscriberState::WaitMessage => {
                unsafe { self.read_buffer.set_len(self.read_buffer.len()+res) };
                let line = self.read_line().expect("There should be one read by now");
                let msg = std::str::from_utf8(line.as_ref()).unwrap();
                info!("Got msg: {msg}");
                return Ok(ConnectionResult::Done);
            },
            _ => panic!("Shouldn't have seen this...")
        }
        Ok(ConnectionResult::Continue)
    }
}

impl Connection for Subscriber {
    fn handle(&mut self, op: Op, res: i32) -> anyhow::Result<ConnectionResult> {
        match op {
            Op::Recv => {
                info!("Subscriber Recv {res}");
                self.recv(res.try_into().unwrap())
            },
            Op::Send => {
                info!("Subscriber Send {res}");
                self.send_ack(res.try_into().unwrap())
            },
            _ => panic!("unexpected op {op}")
        }
    }

    fn shutdown(&self) -> anyhow::Result<()> {
        info!("Shutting down subscriber");
        Ok(self.conn.shutdown(std::net::Shutdown::Both)?)
    }
}

fn main() -> anyhow::Result<()> {
    env_logger::init();

    uring::init(uring::UringArgs{
        uring_size: 1024,
        submissions_threshold: 8,
        sqpoll_interval_ms: 0,
    })?;

    // arm a heartbeat timeout
    let timeout_ud = UserData::new(Op::Timeout, 0);
    let ts = types::Timespec::new().sec(5).nsec(0);
    let timeout = opcode::Timeout::new(&ts as *const types::Timespec)
        .flags(io_uring::types::TimeoutFlags::MULTISHOT)
        .count(0)
        .build()
        .user_data(timeout_ud.into_u64());
    uring::submit(timeout).expect("arm timeout");

    // Set up my connections
    let mut connections: HashMap<RawFd, Box<dyn Connection>> = HashMap::new();

    // Subscriber first so someone can receive
    let mut s = Subscriber::new();
    info!("Subscriber {}", s.fd);
    s.send_hi()?;
    connections.insert(s.fd, Box::new(s));

    std::thread::sleep(std::time::Duration::from_millis(50));

    // Then publisher who will send
    let mut p = Publisher::new();
    info!("Publisher {}", p.fd);
    p.send_hi()?;
    connections.insert(p.fd, Box::new(p));

    if let Err(e) = uring::run(move |ud, res, flags| {
        let ud = UserData::try_from(ud).map_err(|_| anyhow::anyhow!("failed userdata extract"))?;
        let fd = ud.fd();
        let op = ud.op().map_err(|e| anyhow::anyhow!("unknown op code {e}"))?;
        match op {
            Op::Timeout => {
                if res != -62 {
                    warn!("Timeout result not 62: {}", res);
                }

                let stats = uring::stats()?;
                info!("Metrics: {}", stats);
            },
            Op::Recv => {
                trace!("Recv CQE result={} flags={} user_data={}", res, flags, ud);
                if res == -1 {
                    error!("Request error: {}", fd);
                    connections.remove(&fd);
                    return Ok(());
                }

                if res < 0 {
                    let error = match -res {
                        libc::EFAULT => "efault",
                        libc::EPIPE => "epipe",
                        libc::EIO => "eio",
                        libc::EINVAL => "einval",
                        libc::EBADF => "ebadf",
                        104 => "connection reset by peer",
                        _ => "other",
                    };
                    error!("Error reading: {} {} {}", fd, res, error);
                    connections.remove(&fd);
                    return Ok(());
                }

                // Get the request out of our outstanding connections hashmap
                let conn = match connections.get_mut(&fd) {
                    Some(r) => r,
                    None => {
                        warn!("No outstanding request for flags: {} result: {} ud: {}", flags, res, ud);
                        return Ok(());
                    },
                };

                match conn.handle(op, res) {
                    Ok(ConnectionResult::Done) => {
                        info!("Removed conn {fd}");
                        conn.shutdown()?;
                        connections.remove(&fd);
                    },
                    Ok(_) => trace!("handled"),
                    Err(e) => {
                        error!("Error when handling: {}", e);
                    return Ok(());
                    }
                };
            },
            Op::Send => {
                trace!("Send CQE result={} flags={} user_data={}", res, flags, ud);
                if res == -1 {
                    error!("Request error: {}", fd);
                    connections.remove(&fd);
                    return Ok(());
                }
                if res == 0 {
                    connections.remove(&fd);
                    return Ok(());
                }
                if res < 0 {
                    let error = match -res {
                        libc::EFAULT => "efault",
                        libc::EPIPE => "epipe",
                        libc::EIO => "eio",
                        libc::EINVAL => "einval",
                        libc::EBADF => "ebadf",
                        104 => "connection reset by peer",
                        _ => "other",
                    };
                    error!("Error writing: {} {} {}", fd, res, error);
                    connections.remove(&fd);
                    return Ok(());
                }

                // Get the request out of our outstanding connections hashmap
                let conn = match connections.get_mut(&fd) {
                    Some(r) => r,
                    None => {
                        warn!("No outstanding request for flags: {} result: {} ud: {}", flags, res, ud);
                        return Ok(());
                    },
                };

                match conn.handle(op, res) {
                    Ok(ConnectionResult::Done) => {
                        debug!("Closing after complete send");
                        conn.shutdown()?;
                        connections.remove(&fd);
                    },
                    Ok(_) => trace!("handled"),
                    Err(e) => {
                        error!("Error when handling: {}", e);
                        return Ok(());
                    }
                };
            },
            _ => {
                warn!("Unrecognized opcode cqe flags={} result={} user_data={}", flags, res, ud);
            },
        }
        Ok(())
    }, || {}) {
        error!("Error running uring: {}", e);
    }
    Ok(())
}
