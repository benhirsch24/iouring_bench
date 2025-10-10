use clap::{Parser};
use io_uring::{opcode, types};
use log::{error, info};

use std::collections::HashMap;
use std::os::fd::AsRawFd;
use std::rc::Rc;

pub mod user_data;
use user_data::{Op, UserData};

pub mod request;
use request::*;

pub mod uring;

const SILLY_TEXT: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/assets/silly_text.txt"));

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Chunk size to send the file in chunks of
    #[arg(short, long, default_value_t = 4096)]
    chunk_size: usize,

    /// Size of uring submission queue
    #[arg(short, long, default_value_t = 4096)]
    uring_size: u32,

    /// Number of submissions in the backlog before submitting to uring
    #[arg(short, long, default_value_t = 1024)]
    submissions_threshold: usize,

    /// Interval for kernel submission queue polling. If 0 then sqpoll is disabled. Default 0.
    #[arg(short = 'i', long, default_value_t = 0)]
    sqpoll_interval_ms: u32,
}

fn main() -> Result<(), std::io::Error> {
    env_logger::init();
    let args = Args::parse();
    info!("{args:?}");

    // Here's our super simple statically allocated cache
    let mut cache = Rc::new(HashMap::<String, String>::new());
    Rc::get_mut(&mut cache).unwrap().insert("1".to_string(), SILLY_TEXT.to_string());

    uring::init(uring::UringArgs{
        uring_size: args.uring_size,
        submissions_threshold: args.submissions_threshold,
        sqpoll_interval_ms: args.sqpoll_interval_ms,
    })?;

    // Arm multi-shot accept so we don't have to continually resubmit
    let listener = std::net::TcpListener::bind("0.0.0.0:8080").expect("tcp listener");
    let lfd = types::Fd(listener.as_raw_fd());
    let accept_ud = UserData::new(Op::Accept, 0);
    let accept_e = opcode::AcceptMulti::new(lfd).build().user_data(accept_ud.into_u64());
    uring::submit(accept_e).expect("arm accept");

    // Add the first timeout
    let timeout_ud = UserData::new(Op::Timeout, 0);
    let ts = types::Timespec::new().sec(5).nsec(0);
    let timeout = opcode::Timeout::new(&ts as *const types::Timespec)
        .flags(io_uring::types::TimeoutFlags::MULTISHOT)
        .count(0)
        .build()
        .user_data(timeout_ud.into_u64());
    uring::submit(timeout).expect("arm timeout");

    if let Err(e) = uring::run(move |fd| CachingRequest::new(fd, cache.clone(), args.chunk_size) ) {
        error!("Error running uring: {}", e);
    }
    Ok(())
}
