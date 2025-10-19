use bytes::{BufMut, BytesMut};
use clap::{Parser};
use io_uring::{opcode, types};
use log::{error, info, trace, warn};
use std::io::Write;

use iouring_bench::callbacks::*;
use iouring_bench::uring;
use iouring_bench::net as unet;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
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

fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();

    uring::init(uring::UringArgs{
        uring_size: args.uring_size,
        submissions_threshold: args.submissions_threshold,
        sqpoll_interval_ms: args.sqpoll_interval_ms,
    })?;

    let executor = executor::Executor::new();

    executor::spawn(async {
        let listener = unet::TcpListener::bind("0.0.0.0:8080")?;
        loop {
            let stream = listener.accept().await?;
            info!("Got stream: {}", stream.as_raw_fd());
        }
    });

    Ok(())
}
