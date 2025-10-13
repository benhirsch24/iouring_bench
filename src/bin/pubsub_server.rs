use clap::{Parser};
use io_uring::{opcode, types};
use log::{error, info, trace, warn};

use std::os::fd::AsRawFd;

use iouring_bench::callbacks::*;
use iouring_bench::uring;

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

    // arm a heartbeat timeout
    let ts = types::Timespec::new().sec(5).nsec(0);
    let timeout = opcode::Timeout::new(&ts as *const types::Timespec)
        .flags(io_uring::types::TimeoutFlags::MULTISHOT)
        .count(0)
        .build()
        .user_data(0);
    uring::submit(timeout).expect("arm timeout");

    let listener = std::net::TcpListener::bind("0.0.0.0:8080").expect("tcp listener");
    let lfd = types::Fd(listener.as_raw_fd());
    let entry = opcode::AcceptMulti::new(lfd).build();
    submit_entry(entry, move |res| {
        println!("Accepted");
        Ok(())
    })?;

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
