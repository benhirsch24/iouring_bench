use io_uring::{IoUring, squeue::Entry, types};
use histogram::Histogram;
use log::{debug, error, info, trace, warn};

use std::cell::{RefCell, UnsafeCell};
use std::collections::HashMap;
use std::os::fd::RawFd;
use std::rc::Rc;

use crate::request::{Request, RequestState};
use crate::user_data::{Op, UserData};

pub struct UringArgs {
    pub uring_size: u32,
    pub submissions_threshold: usize,
    pub sqpoll_interval_ms: u32,
}

pub struct Uring {
    uring: IoUring,
    args: UringArgs,
    requests: HashMap<RawFd, Box<dyn Request + 'static>>,
    to_submit: Vec<Entry>,
}

// Unsafe cell is used because we are using thread-local storage and there will be only one uring
// active per thread.
// This allows us to call uring::run(...); and then from within the run function or the passed-in
// request factory function call uring::submit(...);
thread_local! {
    static URING: UnsafeCell<Option<Uring>> = UnsafeCell::new(None);
}

pub fn init(args: UringArgs) -> Result<(), std::io::Error> {
    URING.with(|uring| {
        unsafe {
            let uring_ref = &mut *uring.get();
            if uring_ref.is_some() {
                return Ok(());
            }

            let new_uring = Uring::new(args)?;
            *uring_ref = Some(new_uring);
            Ok(())
        }
    })
}

pub fn run<F, R>(request_factory: F) -> Result<(), std::io::Error>
    where
        F: Fn(types::Fd) -> Result<R, std::io::Error>,
        R: Request + 'static
{
    URING.with(|uring| {
        unsafe {
            let uring_ref = &mut *uring.get();
            let uring_mut = uring_ref.as_mut().unwrap();
            uring_mut.run(request_factory)
        }
    })?;
    Ok(())
}

pub fn submit(sqe: Entry) -> Result<(), std::io::Error> {
    URING.with(|uring| {
        unsafe {
            let uring_ref = &mut *uring.get();
            let uring_mut = uring_ref.as_mut().unwrap();
            uring_mut.submit(sqe)
        }
    });
    Ok(())
}

impl Uring {
    fn new(args: UringArgs) -> Result<Uring, std::io::Error> {
        let uring: IoUring<io_uring::squeue::Entry, io_uring::cqueue::Entry> = if args.sqpoll_interval_ms > 0 {
            IoUring::builder()
                .setup_cqsize(args.uring_size*2)
                .setup_sqpoll(args.sqpoll_interval_ms)
                .build(args.uring_size)?
        } else {
            IoUring::builder()
                .setup_cqsize(args.uring_size*2)
                .build(args.uring_size)?
        };
        Ok(Uring {
            uring,
            args,
            requests: HashMap::new(),
            to_submit: Vec::new(),
        })
    }

    fn run<F, R>(&mut self, request_factory: F) -> Result<(), std::io::Error>
    where
        F: Fn(types::Fd) -> Result<R, std::io::Error>,
        R: Request + 'static
    {
        let mut submitted_last_period = 0;
        let mut completions_last_period = 0;
        let mut submit_and_wait = 0;
        let mut last_submit = std::time::Instant::now();
        let mut submit_and_wait_batch_size = Histogram::new(7, 64).expect("sawbs histo");
        let write_timing_histogram = Rc::new(RefCell::new(Histogram::new(7, 64).expect("histogram")));

        loop {
            let mut completed = 0;

            // Check for completions
            self.uring.completion().sync();
            for e in self.uring.completion() {
                completions_last_period += 1;
                completed += 1;
                trace!("completion result={} ud={}", e.result(), e.user_data());
                let ud = UserData::try_from(e.user_data()).expect("failed userdata extract");
                let fd = ud.fd();
                let op = ud.op().expect("op");
                match op {
                    Op::Timeout => {
                        if e.result() != -62 {
                            warn!("Timeout result not 62: {}", e.result());
                        }

                        info!("Metrics: submitted_last_period={submitted_last_period} completions_last_period={completions_last_period} submit_and_wait={submit_and_wait} backlog={}", self.to_submit.len());

                        info!("submit_and_wait batch sizes");
                        let percentiles = [0.0, 50.0, 90.0, 99.0, 99.9, 99.99, 100.0];
                        if let Some(ps) = submit_and_wait_batch_size.percentiles(&percentiles).expect("saw percentiles") {
                            for p in ps {
                                info!("p{} range={:?} count={}", p.0, p.1.range(), p.1.count());
                            }
                        }

                        let percentiles = [0.0, 50.0, 90.0, 99.0, 100.0];
                        if let Some(ps) = write_timing_histogram.borrow().percentiles(&percentiles).expect("percentiles") {
                            debug!("Median time betwen writes for a request={}", ps[1].1.count());
                        }

                        submitted_last_period = 0;
                        completions_last_period = 0;
                        submit_and_wait = 0;
                    },
                    Op::Accept => {
                        if e.result() == 127 {
                            error!("Failed to accept");
                            continue;
                        }
                        trace!("Accept! flags: {} result: {} ud: {}", e.flags(), e.result(), e.user_data());

                        // Create a new request object around this file descriptor and enqueue the
                        // first read
                        let req = match request_factory(types::Fd(e.result())) {
                            Ok(r) => Box::new(r),
                            Err(e) => {
                                error!("Failed to create request: {}", e);
                                continue;
                            }
                        };
                        self.requests.insert(e.result(), req);
                    },
                    Op::Recv => {
                        trace!("Recv CQE result={} flags={} user_data={}", e.result(), e.flags(), e.user_data());
                        if e.result() == -1 {
                            error!("Request error: {}", fd);
                            unsafe { libc::close(fd); };
                            self.requests.remove(&fd);
                            continue;
                        }
                        if e.result() == 0 {
                            unsafe { libc::close(fd); };
                            self.requests.remove(&fd);
                            continue;
                        }
                        if e.result() < 0 {
                            let error = match -e.result() {
                                libc::EFAULT => "efault",
                                libc::EPIPE => "epipe",
                                libc::EIO => "eio",
                                libc::EINVAL => "einval",
                                libc::EBADF => "ebadf",
                                104 => "connection reset by peer",
                                _ => "other",
                            };
                            error!("Error reading: {} {} {}", fd, e.result(), error);
                            unsafe { libc::close(fd); };
                            self.requests.remove(&fd);
                            continue;
                        }

                        // Get the request out of our outstanding requests hashmap
                        let req = match self.requests.get_mut(&fd) {
                            Some(r) => r,
                            None => {
                                warn!("No outstanding request for flags: {} result: {} ud: {}", e.flags(), e.result(), e.user_data());
                                continue;
                            },
                        };

                        match req.handle(op, e.result()) {
                            Ok(RequestState::Done) => {
                                unsafe { libc::close(fd); };
                                self.requests.remove(&fd);
                            },
                            Ok(_) => trace!("handled"),
                            Err(e) => {
                                error!("Error when handling: {}", e);
                                continue;
                            }
                        };
                    },
                    Op::Send => {
                        trace!("Send CQE result={} flags={} user_data={}", e.result(), e.flags(), e.user_data());
                        if e.result() == -1 {
                            error!("Request error: {}", fd);
                            unsafe { libc::close(fd); };
                            self.requests.remove(&fd);
                            continue;
                        }
                        if e.result() == 0 {
                            unsafe { libc::close(fd); };
                            self.requests.remove(&fd);
                            continue;
                        }
                        if e.result() < 0 {
                            let error = match -e.result() {
                                libc::EFAULT => "efault",
                                libc::EPIPE => "epipe",
                                libc::EIO => "eio",
                                libc::EINVAL => "einval",
                                libc::EBADF => "ebadf",
                                104 => "connection reset by peer",
                                _ => "other",
                            };
                            error!("Error writing: {} {} {}", fd, e.result(), error);
                            unsafe { libc::close(fd); };
                            self.requests.remove(&fd);
                            continue;
                        }

                        // Get the request out of our outstanding requests hashmap
                        let req = match self.requests.get_mut(&fd) {
                            Some(r) => r,
                            None => {
                                warn!("No outstanding request for flags: {} result: {} ud: {}", e.flags(), e.result(), e.user_data());
                                continue;
                            },
                        };

                        match req.handle(op, e.result()) {
                            Ok(RequestState::Done) => {
                                unsafe { libc::close(fd); };
                                self.requests.remove(&fd);
                            },
                            Ok(_) => trace!("handled"),
                            Err(e) => {
                                error!("Error when handling: {}", e);
                                continue;
                            }
                        };
                    },
                    _ => {
                        warn!("Unrecognized opcode cqe flags={} result={} user_data={}", e.flags(), e.result(), e.user_data());
                    },
                }
            }

            // Submit N batches of size threshold
            let num_batches = self.to_submit.len() / self.args.submissions_threshold;
            for _ in 0..num_batches {
                let batch: Vec<io_uring::squeue::Entry> = self.to_submit.drain(0..self.args.submissions_threshold).collect();
                unsafe { self.uring.submission().push_multiple(&batch) }.expect("push multiple");
                self.uring.submit().expect("Submitted");
                last_submit = std::time::Instant::now();
                submitted_last_period += 1;
            }

            // Submit and wait for a completion when:
            // 1. We're in SQPOLL mode and we haven't submitted in over the interval, so the kernel
            //    thread may have returned.
            // 2. We haven't submitted in some period of time because we haven't hit the batch threshold
            let should_sqpoll_submit = self.args.sqpoll_interval_ms > 0 && last_submit.elapsed().as_millis() > self.args.sqpoll_interval_ms.into();
            if should_sqpoll_submit || completed == 0 {
                let batch: Vec<io_uring::squeue::Entry> = self.to_submit.drain(..).collect();
                unsafe { self.uring.submission().push_multiple(&batch) }.expect("push multiple");
                debug!("Submit and wait last_submit={} backlog={} batch_size={} t_diff={}", last_submit.elapsed().as_millis(), self.to_submit.len(), batch.len(), last_submit.elapsed().as_micros());
                submit_and_wait_batch_size.increment(batch.len() as u64).unwrap();
                submit_and_wait += 1;
                last_submit = std::time::Instant::now();
                self.uring.submit_and_wait(1)?;
            }
        }
        Ok(())
    }

    fn submit(&mut self, sqe: Entry) {
        self.to_submit.push(sqe);
    }
}
