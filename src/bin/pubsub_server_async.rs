use clap::{Parser};
use log::{error, info, trace, warn};

use iouring_bench::executor;
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

    executor::init();

    executor::spawn(Box::pin(async {
        let mut listener = unet::TcpListener::bind("0.0.0.0:8080").unwrap();
        loop {
            let task_id = executor::get_task_id();
            info!("Accepting {task_id}");
            let stream = listener.accept_multi_fut().await.unwrap();
            executor::spawn(Box::pin(async move {
                let task_id = executor::get_task_id();
                info!("Got stream {} for {task_id}", stream.as_raw_fd());
            }));
        }
        ()
    }));

    executor::run();

    Ok(())
}
