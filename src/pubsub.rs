use std::cell::RefCell;
use std::collections::HashMap;
use std::os::fd::RawFd;
use std::rc::Rc;

use crate::user_data::{Op, UserData};

use bytes::Bytes;
use io_uring::{opcode, squeue::Entry, types};

pub struct PubsubState {
    subscribers: HashMap<String, Vec<RawFd>>,
}

impl PubsubState {
    pub fn new() -> Self {
        PubsubState {
            subscribers: HashMap::new(),
        }
    }

    pub fn subscribe(&mut self, channel: String, fd: RawFd) {
        self.subscribers.entry(channel)
            .or_insert_with(Vec::new)
            .push(fd);
    }

    pub fn get_subscribers(&self, channel: &String) -> Vec<RawFd> {
        if !self.subscribers.contains_key(channel) {
            return Vec::new()
        }
        self.subscribers.get(channel).unwrap().clone()
    }
}

pub struct Buffer {
    subscribers: HashMap<RawFd, BufferSubscriber>,
}

impl Buffer {
    pub fn new(message: String, subscriber_fds: Vec<RawFd>) -> Buffer {
        let buf = Bytes::copy_from_slice(message.as_bytes());
        let mut subscribers = HashMap::new();
        for subscriber in subscriber_fds {
            // Each subscriber has a reference to the buffer
            subscribers.insert(subscriber, BufferSubscriber::new(subscriber, buf.slice(..)));
        }
        Buffer {
            subscribers,
        }
    }

    pub fn get_sends(&mut self) -> Vec<Entry> {
        let mut entries = vec![];
        for (_, s) in self.subscribers.iter_mut() {
            entries.push(s.send(0).expect("should have first send"));
        }
        entries
    }

    pub fn send(&mut self, fd: RawFd, n: usize) -> Option<Entry> {
        let s = self.subscribers.get_mut(&fd);
        if s.is_none() {
            return None;
        }

        let s = s.unwrap();
        let entry = s.send(n);
        if entry.is_none() {
            self.subscribers.remove(&fd);
        }
        entry
    }

    pub fn is_done(&self) -> bool {
        self.subscribers.is_empty()
    }
}

struct BufferSubscriber {
    fd: types::Fd,
    buf: Bytes,
    sent: usize,
}

impl BufferSubscriber {
    fn new(fd: RawFd, buf: Bytes) -> BufferSubscriber {
        BufferSubscriber {
            fd: types::Fd(fd),
            buf,
            sent: 0,
        }
    }

    fn send(&mut self, n: usize) -> Option<Entry> {
        self.sent += n;
        let start = self.sent;
        let end = self.buf.len();
        if start == end {
            return None;
        }
        let ptr = self.buf.slice(start..end).as_ptr();
        let to_send: u32 = (end - start) as u32;
        let send_e = opcode::Send::new(self.fd, ptr, to_send);
        let ud = UserData::new(Op::Send, self.fd.0);
        Some(send_e.build().user_data(ud.into()))
    }
}

#[derive(Clone)]
pub struct BufferPool {
    inner: Rc<RefCell<HashMap<String, Buffer>>>,
}

impl BufferPool {
    pub fn new() -> BufferPool {
        BufferPool {
            inner: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    pub fn register(&mut self, channel: String, buf: Buffer) {
        self.inner.borrow_mut().insert(channel, buf);
    }

    pub fn get_send_for_buffer(&mut self, channel: &String, fd: RawFd, n: usize) -> Option<Entry> {
        let mut i = self.inner.borrow_mut();
        let b = i.get_mut(channel).unwrap();
        b.send(fd, n)
    }

    pub fn is_done(&self, channel: &String) -> bool {
        let i = self.inner.borrow();
        i.get(channel).unwrap().is_done()
    }

    pub fn remove(&mut self, channel: &String) {
        self.inner.borrow_mut().remove(channel);
    }
}
