use io_uring::{IoUring, squeue::Entry};
use histogram::Histogram;
use log::{error, trace};

use std::cell::UnsafeCell;

use crate::user_data::UserData;

pub struct UringArgs {
    pub uring_size: u32,
    pub submissions_threshold: usize,
    pub sqpoll_interval_ms: u32,
}

#[derive(Clone)]
pub struct UringStats {
        pub submitted_last_period: u64,
        pub completions_last_period: u64,
        pub submit_and_wait: u64,
        pub to_submit_backlog: usize,
        pub last_submit: std::time::Instant,
        pub submit_and_wait_batch_size: Histogram,
}

impl std::fmt::Display for UringStats {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "submitted_last_period={} completions_last_period={} submit_and_wait={} last_submit={}us",
            self.submitted_last_period, self.completions_last_period, self.submit_and_wait,
            self.last_submit.elapsed().as_micros())
    }
}

impl UringStats {
    fn new() -> UringStats {
        UringStats {
            submitted_last_period: 0,
            completions_last_period: 0,
            submit_and_wait: 0,
            to_submit_backlog: 0,
            last_submit: std::time::Instant::now(),
            submit_and_wait_batch_size: Histogram::new(7, 64).expect("sawbs histo"),
        }
    }
}

pub struct Uring {
    uring: IoUring,
    args: UringArgs,
    to_submit: Vec<Entry>,
    stats: UringStats,
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

pub fn run<H>(handler: H) -> Result<(), anyhow::Error>
    where
        H: FnMut(UserData, i32, u32) -> Result<(), anyhow::Error>,
{
    URING.with(|uring| {
        unsafe {
            let uring_ref = &mut *uring.get();
            let uring_mut = uring_ref.as_mut().unwrap();
            uring_mut.run(handler)
        }
    })?;
    Ok(())
}

pub fn submit(sqe: Entry) -> Result<(), std::io::Error> {
    URING.with(|uring| {
        unsafe {
            let uring_ref = &mut *uring.get();
            match uring_ref.as_mut() {
                Some(u) => u.submit(sqe),
                None => panic!("uring not initialized")
            }
        }
    });
    Ok(())
}

pub fn stats() -> Result<UringStats, std::io::Error> {
    URING.with(|uring| {
        unsafe {
            let uring_ref = &mut *uring.get();
            let uring_mut = uring_ref.as_mut().unwrap();
            let mut stats = uring_mut.stats.clone();
            stats.to_submit_backlog = uring_mut.to_submit.len();
            uring_mut.stats = UringStats::new();
            Ok(stats)
        }
    })
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
            to_submit: Vec::new(),
            stats: UringStats::new(),
        })
    }

    fn run<H>(&mut self, mut handler: H) -> Result<(), anyhow::Error>
    where
        H: FnMut(UserData, i32, u32) -> Result<(), anyhow::Error>,
    {
        loop {
            let mut completed = 0;

            // Check for completions
            self.uring.completion().sync();
            for e in self.uring.completion() {
                self.stats.completions_last_period += 1;
                completed += 1;
                trace!("completion result={} ud={}", e.result(), e.user_data());
                let ud = UserData::try_from(e.user_data()).expect("failed userdata extract");
                if let Err(err) = handler(ud, e.result(), e.flags()) {
                    error!("Error handling cqe (fd={} res={}): {err}", ud.fd(), e.result());
                }
            }

            // Submit N batches of size threshold
            let num_batches = self.to_submit.len() / self.args.submissions_threshold;
            for _ in 0..num_batches {
                let batch: Vec<io_uring::squeue::Entry> = self.to_submit.drain(0..self.args.submissions_threshold).collect();
                unsafe { self.uring.submission().push_multiple(&batch) }.expect("push multiple");
                self.uring.submit().expect("Submitted");
                self.stats.last_submit = std::time::Instant::now();
                self.stats.submitted_last_period += 1;
            }

            // Submit and wait for a completion when:
            // 1. We're in SQPOLL mode and we haven't submitted in over the interval, so the kernel
            //    thread may have returned.
            // 2. We haven't submitted in some period of time because we haven't hit the batch threshold
            let should_sqpoll_submit = self.args.sqpoll_interval_ms > 0 && self.stats.last_submit.elapsed().as_millis() > self.args.sqpoll_interval_ms.into();
            if should_sqpoll_submit || completed == 0 {
                let batch: Vec<io_uring::squeue::Entry> = self.to_submit.drain(..).collect();
                unsafe { self.uring.submission().push_multiple(&batch) }.expect("push multiple");
                trace!("Submit and wait last_submit={} backlog={} batch_size={} t_diff={}", self.stats.last_submit.elapsed().as_millis(), self.to_submit.len(), batch.len(), self.stats.last_submit.elapsed().as_micros());
                self.stats.submit_and_wait_batch_size.increment(batch.len() as u64).unwrap();
                self.stats.submit_and_wait += 1;
                self.stats.last_submit = std::time::Instant::now();
                self.uring.submit_and_wait(1)?;
            }
        }
        Ok(())
    }

    fn submit(&mut self, sqe: Entry) {
        self.to_submit.push(sqe);
    }
}
