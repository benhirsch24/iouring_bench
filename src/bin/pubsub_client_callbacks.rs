use io_uring::{opcode, types};
use log::{error, info, warn};
use bytes::{BufMut, Bytes, BytesMut};

use std::collections::HashMap;
use std::cell::{RefCell, UnsafeCell};
use std::net::TcpStream;
use std::os::fd::{AsRawFd, RawFd};
use std::rc::Rc;

use iouring_bench::uring;
use iouring_bench::user_data::{Op, UserData};

struct PublisherInner {
    conn: TcpStream,
    fd: RawFd,
    read_buffer: BytesMut,
    send_buffer: BytesMut,
}

impl PublisherInner {
    fn read_line(&mut self) -> Option<Bytes> {
        let buf = &mut self.read_buffer;
        for idx in 0..buf.len() {
            if buf[idx] == b'\n' && buf[idx-1] == b'\r' {
                let b = buf.split_to(idx+1).freeze();
                return Some(b);
            }
        }
        None
    }
}

#[derive(Clone)]
struct Publisher {
    inner: Rc<RefCell<PublisherInner>>,
}

impl Publisher {
    fn new() -> Publisher {
        let conn = TcpStream::connect("127.0.0.1:8080").expect("publish conn");
        let fd = conn.as_raw_fd();
        Publisher {
            inner: Rc::new(RefCell::new(PublisherInner {
                conn: conn,
                fd,
                read_buffer: BytesMut::with_capacity(1024),
                send_buffer: BytesMut::with_capacity(1024),
            }))
        }
    }

    fn send_hi(&mut self) -> anyhow::Result<()> {
        let send_e = {
            let mut inner = self.inner.borrow_mut();
            inner.send_buffer.put_slice(b"PUBLISH ch\r\n");
            let ptr = inner.send_buffer.as_ptr();
            let to_send = inner.send_buffer.len();
            opcode::Send::new(types::Fd(inner.fd), ptr, to_send.try_into().unwrap())
        };
        let ud = add_callback({
            let inner = self.inner.clone();
            move |res| {
                // TODO: What if send is bigger than res?
                info!("Send completed! res={res}");
                let read_e = {
                    let mut inner = inner.borrow_mut();
                    let ptr = inner.read_buffer.as_mut_ptr();
                    opcode::Recv::new(types::Fd(inner.fd), ptr, (inner.read_buffer.capacity() - inner.read_buffer.len()) as u32)
                };
                let ud = add_callback({
                    let inner = inner.clone();
                    move |res: i32| {
                        let send_e = {
                            let mut inner = inner.borrow_mut();
                            info!("Received res={res}");
                            let newlen: usize = inner.read_buffer.len() + (res as usize);
                            unsafe {
                                inner.read_buffer.set_len(newlen)
                            };
                            let line = inner.read_line().expect("There should be one read by now");
                            if line != "OK\r\n" {
                                anyhow::bail!("Expected OK");
                            }
                            // Split removes anything that's been filled already. Put our message in
                            // to send instead.
                            let _ = inner.send_buffer.split();
                            inner.send_buffer.put_slice(b"hello\r\n");
                            let ptr = inner.send_buffer.as_ptr();
                            let to_send = inner.send_buffer.len();
                            opcode::Send::new(types::Fd(inner.fd), ptr, to_send.try_into().unwrap())
                        };
                        let ud = add_callback({
                            let inner = inner.clone();
                            move |_res| {
                                info!("Done!");
                                inner.borrow_mut().conn.shutdown(std::net::Shutdown::Both)?;
                                Ok(())
                            }
                        });
                        uring::submit(send_e.build().user_data(ud.into()))?;
                        Ok(())
                    }
                });
                let e = read_e.build().user_data(ud).into();
                uring::submit(e)?;
                Ok(())
            }
        });
        uring::submit(send_e.build().user_data(ud))?;
        info!("Sending hi ud={ud}");
        Ok(())
    }
}

struct CallbackRegistry {
    map: HashMap<u64, Box<dyn FnOnce(i32) -> anyhow::Result<()>>>,
    counter: u64,
}

impl CallbackRegistry {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            // TODO: Reserving special user data 0 for the metrics timeout (for now, until I
            // write a more scalable solution
            counter: 1,
        }
    }

    fn add_callback<T>(&mut self, f: T) -> u64
    where
    T: FnOnce(i32) -> anyhow::Result<()> + 'static
    {
        let c = self.counter;
        self.map.insert(c, Box::new(f));
        self.counter += 1;
        c
    }

    fn call_back(&mut self, id: u64, res: i32) -> anyhow::Result<()> {
        let cb = self.map.remove(&id);
        match cb {
            Some(cb) => cb(res),
            None => anyhow::bail!("No callback registered for {id} res={res}")
        }
    }
}

thread_local! {
    static CB: UnsafeCell<CallbackRegistry> = UnsafeCell::new(CallbackRegistry::new());
}

pub fn add_callback<F>(f: F) -> u64
    where
        F: FnOnce(i32) -> anyhow::Result<()> + 'static
{
    CB.with(|cbr| {
        unsafe {
            let cb_ref = &mut *cbr.get();
            cb_ref.add_callback(Box::new(f))
        }
    })
}

pub fn call_back(id: u64, res: i32) -> anyhow::Result<()> {
    CB.with(|cbr| {
        unsafe {
            let cb_ref = &mut *cbr.get();
            cb_ref.call_back(id, res)
        }
    })
}

fn main() -> anyhow::Result<()> {
    env_logger::init();

    uring::init(uring::UringArgs{
        uring_size: 1024,
        submissions_threshold: 8,
        sqpoll_interval_ms: 0,
    })?;

    // arm a heartbeat timeout
    let ts = types::Timespec::new().sec(5).nsec(0);
    let timeout = opcode::Timeout::new(&ts as *const types::Timespec)
        .flags(io_uring::types::TimeoutFlags::MULTISHOT)
        .count(0)
        .build()
        .user_data(0);
    uring::submit(timeout).expect("arm timeout");

    // Then publisher who will send
    let mut p = Publisher::new();
    info!("Publisher {}", p.inner.borrow().fd);
    p.send_hi()?;

    if let Err(e) = uring::run(move |ud, res, _flags| {
        info!("Got completion event ud={ud} res={res}");
        match ud {
            // Timeout
            0 => {
                if res != -62 {
                    warn!("Timeout result not 62: {}", res);
                }

                let stats = uring::stats()?;
                info!("Metrics: {}", stats);
            },
            // Callback registry
            _ => {
                call_back(ud, res)?
            }
        }
        Ok(())
    }) {
        error!("Error running uring: {}", e);
    }
    Ok(())
}
