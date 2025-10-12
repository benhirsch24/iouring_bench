use std::collections::HashMap;
use std::os::fd::RawFd;

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
