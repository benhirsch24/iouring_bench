use bytes::{Bytes, BytesMut};
use io_uring::{opcode, squeue::Entry, types};
use log::{warn};

use std::collections::HashMap;
use std::rc::Rc;

use crate::user_data::{Op, UserData};
use crate::uring::submit;

static BUFFER_SIZE : usize = 1024*1024;

const NOT_FOUND: &'static str = "HTTP/1.1 404 Not Found\r\nContent-Length: 8\r\n\r\nNotFound";
const HEALTH_OK: &'static str = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";

pub enum RequestState {
    Continue,
    Done,
}

pub trait Request {
    fn handle(&mut self, op: Op, res: i32) -> Result<RequestState, std::io::Error>;
}

pub struct CachingRequest {
    fd: types::Fd,
    buffer: BytesMut,
    response: Option<Bytes>,
    cache: Rc<HashMap<String, String>>,
    sent: usize,
    chunk_size: usize,
    //write_timing_histogram: Rc<RefCell<Histogram>>,
    last_write: Option<std::time::Instant>,
}

impl CachingRequest {
    pub fn new(fd: types::Fd, cache: Rc<HashMap<String, String>>, /*write_timing_histogram: Rc<RefCell<Histogram>>,*/ chunk_size: usize) -> CachingRequest {
        CachingRequest {
            fd,
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
            response: None,
            cache,
            sent: 0,
            chunk_size,
            //write_timing_histogram,
            last_write: None,
        }
    }

    pub fn is_complete(&self) -> bool {
        self.sent == self.response.as_ref().unwrap().len()
    }

    pub fn set_response(&mut self, resp: &str) {
        // Store response in this request as it needs to be allocated until the kernel has sent it
        // Eventually this could be a pointer to kernel owned buffer
        self.response = Some(Bytes::copy_from_slice(resp.as_bytes()));
    }

    pub fn read(&mut self) -> Entry {
        let ptr = unsafe { self.buffer.as_mut_ptr().add(self.buffer.len()) };
        let read_e = opcode::Recv::new(self.fd, ptr, (self.buffer.capacity() - self.buffer.len()) as u32);
        let ud = UserData::new(Op::Recv, self.fd.0);
        return read_e.build().user_data(ud.into_u64()).into();
    }

    fn advance_read(&mut self, n: usize) {
        unsafe { self.buffer.set_len(self.buffer.len() + n) };
    }

    fn send(&mut self, n: usize) -> Option<Entry> {
        self.sent += n;
        let len = self.response.as_ref().unwrap().len();
        if self.sent == len {
            return None;
        }

        let start = self.sent;
        let end = if start + self.chunk_size > len { len } else { start + self.chunk_size };
        let ptr = self.response.as_ref().unwrap().slice(start..end).as_ptr();
        let to_send : u32 = (end - start) as u32;
        let send_e = opcode::Send::new(self.fd, ptr, to_send);
        let ud = UserData::new(Op::Send, self.fd.0);

        // Stats on time between writes
        //if let Some(n) = self.last_write {
        //    self.write_timing_histogram.borrow_mut().increment(n.elapsed().as_micros() as u64).expect("increment");
        //}
        self.last_write = Some(std::time::Instant::now());

        Some(send_e.build().user_data(ud.into_u64()).into())
    }

    fn serve(&mut self) -> Vec<Entry> {
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut r = httparse::Request::new(headers.as_mut_slice());
        let request = r.parse(&self.buffer[..]).expect("parse error");
        let mut sqe = Vec::new();

        // Not finished with request, keep reading
        if !request.is_complete() {
            let e = self.read();
            sqe.push(e);
            return sqe;
        }

        // We have a request, let's route
        match r.path {
            Some(path) => {
                match path {
                    p if p.starts_with("/object") => {
                        let parts = p.split("/").collect::<Vec<_>>();
                        if parts.len() != 3 {
                            self.set_response(&format!("HTTP/1.1 400 Bad Request\r\nContent-Length: 25\r\n\r\nExpected /object/<object>"));
                            let send = self.send(0).unwrap();
                            sqe.push(send);
                        } else {
                            if let Some(o) = self.cache.get(&parts[2].to_string()) {
                                self.set_response(&format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}", o.len(), o));
                                let send = self.send(0).unwrap();
                                sqe.push(send);
                            } else {
                                self.set_response(NOT_FOUND);
                                let send = self.send(0).unwrap();
                                sqe.push(send);
                            }
                        }
                    },
                    "/health" => {
                        self.set_response(HEALTH_OK);
                        let send = self.send(0).unwrap();
                        sqe.push(send);
                    },
                    _ => {
                        self.set_response(NOT_FOUND);
                        let send = self.send(0).unwrap();
                        sqe.push(send);
                    }
                }
            },
            None => {
                self.set_response(NOT_FOUND);
                let send = self.send(0);
                sqe.push(send.unwrap());
            }
        }

        sqe
    }
}

impl Request for CachingRequest {
    fn handle(&mut self, op: Op, result: i32) -> Result<RequestState, std::io::Error> {
        match op {
            Op::Recv => {
                // Advance the request internal offset pointer by how many bytes were read
                // (the result value of the read call)
                self.advance_read(result as usize);

                // Parse the request and submit any calls to the submission queue
                let entries = self.serve();
                for e in entries {
                    submit(e).expect("push serve");
                }
            },
            Op::Send => {
                // TODO: No keep alive implemented yet
                match self.send(result.try_into().unwrap()) {
                    Some(entry) => submit(entry).expect("push write"),
                    None => {
                        return Ok(RequestState::Done);
                    }
                }
            },
            _ => warn!("Didn't expect op {:?}", op)
        }
        Ok(RequestState::Continue)
    }
}
