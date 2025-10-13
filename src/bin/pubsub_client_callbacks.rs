use io_uring::{opcode, types};
use log::{debug, error, info, trace, warn};
use bytes::{BufMut, Bytes, BytesMut};

use std::collections::HashMap;
use std::cell::{UnsafeCell};
use std::net::TcpStream;
use std::os::fd::{AsRawFd, RawFd};

use iouring_bench::uring;

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

fn read_line(mut buf: BytesMut) -> Option<Bytes> {
    for idx in 0..buf.len() {
        if buf[idx] == b'\n' && buf[idx-1] == b'\r' {
            let b = buf.split_to(idx+1).freeze();
            return Some(b);
        }
    }
    None
}

fn read_message_recursive(fd: RawFd) -> anyhow::Result<()> {
    let mut read = BytesMut::with_capacity(1024);
    let ptr = read.as_mut_ptr();
    let op = opcode::Recv::new(types::Fd(fd), ptr, read.capacity() as u32);
    let ud = add_callback(move |res| {
        // Expected message1 completion
        if res < 0 {
            anyhow::bail!("Got negative return from receive: {res} fd={}", fd);
        }
        trace!("Received message res={res}");
        unsafe { read.set_len(res.try_into()?) };
        let line = read_line(read).expect("There should be one message");
        let msg = std::str::from_utf8(line.as_ref())?;
        info!("Subscriber received message {msg}");
        // You lose cancellation but oh well for this example.
        read_message_recursive(fd)
    });
    uring::submit(op.build().user_data(ud))?;
    Ok(())
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

    // Spawn 5 subscribers
    let num_subscribers = 5;
    let mut subscribers = HashMap::new();
    for _ in 0..num_subscribers {
        let subscriber = TcpStream::connect("127.0.0.1:8080")?;
        let fd = subscriber.as_raw_fd();
        subscribers.insert(subscriber.as_raw_fd(), subscriber);
        info!("Sending subscribe fd={}", fd);
        let mut send = BytesMut::with_capacity(1024);
        send.put_slice(b"SUBSCRIBE ch\r\n");
        let ptr = send.as_ptr();
        let n = send.len();
        let op = opcode::Send::new(types::Fd(fd), ptr, n.try_into().unwrap());
        let ud = add_callback(move |res| {
            // Sending SUBSCRIBE completion
            debug!("Sent subscribe res={res}");
            let _ = send.split();
            let mut ok = BytesMut::with_capacity(1024);
            let ptr = ok.as_mut_ptr();
            let op = opcode::Recv::new(types::Fd(fd), ptr, ok.capacity() as u32);
            let ud = add_callback(move |res| {
                // Expected OK receive completion
                debug!("Recv res={res} fd={}", fd);
                unsafe { ok.set_len(res.try_into()?) };
                let line = read_line(ok).expect("There should be one read by now");
                if line != "OK\r\n" {
                    anyhow::bail!("Expected OK");
                }
                debug!("Got OK fd={}", fd);

                read_message_recursive(fd)?;
                Ok(())
            });
            uring::submit(op.build().user_data(ud))?;
            Ok(())
        });
        let e = op.build().user_data(ud);
        uring::submit(e)?;
    }

    std::thread::sleep(std::time::Duration::from_millis(50));

    // Then publisher who will send
    let conn = TcpStream::connect("127.0.0.1:8080").expect("publish conn");
    let fd = conn.as_raw_fd();
    let mut send_buffer = BytesMut::with_capacity(4096);
    send_buffer.put_slice(b"PUBLISH ch\r\n");
    let ptr = send_buffer.as_ptr();
    let to_send = send_buffer.len();
    debug!("Publisher sending");
    let send_e = opcode::Send::new(types::Fd(fd), ptr, to_send.try_into().unwrap());
    let ud = add_callback(move |res| {
        // TODO: What if send is bigger than res?
        info!("Publisher PUBLISH completed res={res}");
        let mut read_buffer = BytesMut::with_capacity(4096);
        let ptr = read_buffer.as_mut_ptr();
        let read_e = opcode::Recv::new(types::Fd(fd), ptr, read_buffer.capacity() as u32);
        let ud = add_callback(move |res: i32| {
            debug!("Publisher OK recv={res}");
            let newlen: usize = read_buffer.len() + (res as usize);
            unsafe {
                read_buffer.set_len(newlen)
            };
            let line = read_line(read_buffer).expect("There should be one read by now");
            if line != "OK\r\n" {
                anyhow::bail!("Expected OK");
            }
            debug!("Publisher received ok");

            let mut send_buffer = BytesMut::with_capacity(4096);
            send_buffer.put_slice(b"hello\r\n");
            let ptr = send_buffer.as_ptr();
            let to_send = send_buffer.len();
            let send_e = opcode::Send::new(types::Fd(fd), ptr, to_send.try_into().unwrap());
            let ud = add_callback(move |_res| {
                info!("Publisher sent message res={res}");
                Ok(())
            });
            uring::submit(send_e.build().user_data(ud.into()))?;
            Ok(())
        });
        let e = read_e.build().user_data(ud).into();
        uring::submit(e)?;
        Ok(())
    });
    uring::submit(send_e.build().user_data(ud))?;
    debug!("Publisher {}", fd);

    if let Err(e) = uring::run(move |ud, res, _flags| {
        trace!("Got completion event ud={ud} res={res}");
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
    info!("Exiting");
    Ok(())
}
