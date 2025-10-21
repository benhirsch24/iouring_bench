use clap::{Parser};
use clap_duration::duration_range_value_parse;
use duration_human::{DurationHuman, DurationHumanValidator};
use futures::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use log::{error, info, trace};

use std::io::Result;
use std::os::fd::AsRawFd;
use std::time::Instant;

use iouring_bench::executor;
use iouring_bench::uring;
use iouring_bench::net as unet;
use iouring_bench::timeout::TimeoutFuture as Timeout;

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

    /// How many publishers to create
    #[arg(short, long, default_value_t = 1)]
    publishers: u32,

    /// How many subscribers per publisher
    #[arg(short = 'm', long, default_value_t = 5)]
    subscribers_per_publisher: u32,

    /// How long to run the test
    #[arg(short = 't', long, default_value = "10s", value_parser = duration_range_value_parse!(min: 1s, max: 1h))]
    timeout: DurationHuman,

    #[arg(short, long, default_value = "127.0.0.1:8080")]
    endpoint: String,
}

async fn handle_publisher(endpoint: String, channel: String, end: Instant) -> Result<()> {
    info!("Connecting task={}", executor::get_task_id());
    let mut stream = unet::TcpStream::connect(endpoint).await?;
    info!("Connected");

    // Inform the server that we're a publisher
    let publish = format!("PUBLISH {channel}\r\n");
    info!("Publish: \"{publish}\"");
    let _ = stream.write_all(publish.as_bytes()).await?;

    // Read back the OK message we expect
    let mut ok = [0u8; 4];
    let _ = stream.read(&mut ok).await?;
    if !ok.starts_with(b"OK\r\n") {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "didn't get OK"));
    }

    // Start publishing
    loop {
        if Instant::now() > end {
            info!("Done!");
            return Ok(());
        }

        let message = "here is my message\r\n";
        let _ = stream.write_all(message.as_bytes()).await?;
        trace!("Wrote message");
    }
}

async fn handle_subscriber(endpoint: String, channel: String, end: Instant) -> Result<()> {
    let mut stream = unet::TcpStream::connect(endpoint).await?;
    let subscribe = format!("SUBSCRIBE {channel}\r\n");
    let _ = stream.write_all(subscribe.as_bytes()).await?;
    let mut ok = [0u8; 16];
    let _ = stream.read(&mut ok).await?;
    if !ok.starts_with(b"OK\r\n") {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "didn't get OK"));
    }

    // Start publishing
    let mut reader = BufReader::new(stream);
    loop {
        if Instant::now() > end {
            info!("Done!");
            return Ok(());
        }

        let mut line = String::new();
        reader.read_line(&mut line).await?;
        trace!("Got line {line}");
    }
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

    executor::spawn(async {
        let mut timeout = Timeout::from_secs(5, true);
        loop {
            timeout = timeout.await.expect("REASON");
            let stats = uring::stats().expect("stats");
            info!("Metrics: {}", stats);
        }
    });

    let start = std::time::Instant::now();
    let end = args.timeout + start;

    for n in 0..args.publishers {
        let channel_name = format!("Channel_{n}");
        // Spawn 1 publisher
        executor::spawn({
            let channel_name = channel_name.clone();
            let endpoint = args.endpoint.clone();
            async move {
                if let Err(e) = handle_publisher(endpoint, channel_name.clone(), end).await {
                    error!("Error on publisher {channel_name} {e}");
                }
                ()
            }
        });

        // Spawn S subscribers
        for _s in 0..args.subscribers_per_publisher {
            executor::spawn({
                let channel_name = channel_name.clone();
                let endpoint = args.endpoint.clone();
                async move {
                    if let Err(e) = handle_subscriber(endpoint, channel_name.clone(), end).await {
                        error!("Error on publisher {channel_name} {e}");
                    }
                    ()
                }
            });
        }
    }

    executor::run();

    Ok(())
}
