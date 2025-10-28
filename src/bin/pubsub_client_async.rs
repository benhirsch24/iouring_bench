use clap::{Parser};
use clap_duration::duration_range_value_parse;
use duration_human::{DurationHuman, DurationHumanValidator};
use futures::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use log::{debug, error, info, trace};
use rand::Rng;

use std::io::Result;
use std::time::{Duration, Instant};

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

    #[arg(long, default_value_t = 20)]
    tps: u32,
}

async fn handle_publisher(tps: u32, endpoint: String, channel: String, end: Instant) -> Result<()> {
    info!("Connecting task={}", executor::get_task_id());
    let mut stream = unet::TcpStream::connect(endpoint).await?;
    info!("Connected publisher {channel}");

    // Inform the server that we're a publisher
    let publish = format!("PUBLISH {channel}\r\n");
    debug!("Publish: \"{publish}\"");
    let _ = stream.write_all(publish.as_bytes()).await?;

    // Read back the OK message we expect
    let mut ok = [0u8; 16];
    debug!("Reading ok");
    let _ = stream.read(&mut ok).await?;
    if !ok.starts_with(b"OK\r\n") {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "didn't get OK"));
    }
    debug!("Got ok");

    // Start publishing
    loop {
        if Instant::now() > end {
            info!("Done!");
            return Ok(());
        }

        let message = format!("here is my message {}\r\n", channel);
        executor::spawn({
            let mut stream = unet::TcpStream::new(stream.as_raw_fd());
            async move {
                trace!("Writing message {message}");
                if let Err(e) = stream.write_all(message.as_bytes()).await {
                    error!("Error writing message {message}: {e}");
                    return;
                }
                trace!("Wrote message");
            }
        });

        let interval = 1000 / tps;
        Timeout::new(Duration::from_millis(interval.into()), false).await?;
    }
}

async fn handle_subscriber(endpoint: String, channel: String, end: Instant) -> Result<()> {
    let mut stream = unet::TcpStream::connect(endpoint).await?;
    info!("Connected subscriber {channel} fd={}", stream.as_raw_fd());
    let subscribe = format!("SUBSCRIBE {channel}\r\n");
    let _ = stream.write_all(subscribe.as_bytes()).await?;
    let mut ok = [0u8; 16];
    debug!("Reading ok");
    let _ = stream.read(&mut ok).await?;
    if !ok.starts_with(b"OK\r\n") {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "didn't get OK"));
    }

    // Start publishing
    let mut reader = BufReader::new(stream);
    let mut last = Instant::now();
    let mut num_msgs = 0;
    let mut total_msgs = 0;
    loop {
        if Instant::now() > end {
            info!("Done! {total_msgs} received");
            return Ok(());
        }

        let mut line = String::new();
        reader.read_line(&mut line).await?;
        num_msgs += 1;
        total_msgs += 1;
        trace!("Got line {line}");

        if last.elapsed() > Duration::from_secs(1) {
            debug!("Channel {channel} received {num_msgs} in last second");
            num_msgs = 0;
            last = Instant::now();
        }
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
        let mut timeout = Timeout::new(Duration::from_secs(5), true);
        loop {
            timeout = timeout.await.expect("REASON");
            let stats = uring::stats().expect("stats");
            info!("Metrics: {}", stats);
        }
    });

    let start = std::time::Instant::now();
    let end = args.timeout + start;

    let mut rng = rand::rng();
    for n in 0..args.publishers {
        let channel_name = format!("Channel_{n}");
        // Add some jitter here for ramp up
        let jitter_ms = rng.random_range(10..100);
        std::thread::sleep(std::time::Duration::from_millis(jitter_ms));

        // Spawn a publisher
        executor::spawn({
            let channel_name = channel_name.clone();
            let endpoint = args.endpoint.clone();
            debug!("Starting publisher for {channel_name}");
            async move {
                if let Err(e) = handle_publisher(args.tps, endpoint, channel_name.clone(), end).await {
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
                debug!("Starting subscriber for {channel_name}");
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
