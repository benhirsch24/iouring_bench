use clap::{Parser};
use log::info;

use futures::io::{AsyncReadExt, AsyncWriteExt};

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
            info!("Accepting");
            let mut stream = listener.accept_multi_fut().unwrap().await.unwrap();
            executor::spawn(Box::pin(async move {
                info!("Got stream {}", stream.as_raw_fd());
                loop {
                    let mut buf = [0u8; 1024];
                    let n = stream.read(&mut buf).await.expect("Read buf");
                    info!("Read {n} from stream");
                    if n == 0 {
                        info!("Stream done");
                        return;
                    }

                    let response = "hello to you too\r\n";
                    let mut written = 0;
                    while written < response.len() {
                        let n = stream.write(response.as_bytes()).await.expect("Write");
                        written += n;
                        info!("Wrote {n}");
                    }
                }
            }));
        }
        ()
    }));

    executor::run();

    Ok(())
}
