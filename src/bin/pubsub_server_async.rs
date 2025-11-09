use clap::Parser;
use futures::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use log::{debug, error, info, trace, warn};

use std::collections::HashMap;
use std::os::fd::RawFd;
use std::time::Duration;
use std::{cell::RefCell, rc::Rc};

use iouring_bench::executor;
use iouring_bench::net as unet;
use iouring_bench::timeout::TimeoutFuture as Timeout;
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

#[derive(Default)]
struct SubscriberInfo {
    streams: HashMap<RawFd, unet::TcpStream>,
    sent: u64,
}

async fn handle_publisher(
    mut reader: BufReader<unet::TcpStream>,
    mut writer: unet::TcpStream,
    channel: String,
    submap: Rc<RefCell<HashMap<String, SubscriberInfo>>>,
) {
    let task_id = executor::get_task_id();
    let fd = writer.as_raw_fd();
    debug!("Handling publisher channel={channel} fd={fd} task_id={task_id}");
    let ok = b"OK\r\n";
    writer.write_all(ok).await.expect("OK");
    trace!("OK fd={fd} task_id={task_id}");

    loop {
        let mut line = String::new();
        match reader.read_line(&mut line).await {
            Ok(n) => {
                if n == 0 {
                    info!("Publisher left");
                    return;
                }
                debug!(
                    "Got message {} channel={channel} fd={fd} task_id={task_id}",
                    line.replace("\n", "\\n").replace("\r", "\\r")
                );
                // TODO: This is ugly af but it works
                let streams = {
                    let mut streams = Vec::new();
                    if let Some(set) = submap.borrow().get(&channel) {
                        for (_, s) in set.streams.iter() {
                            streams.push(unet::TcpStream::new(s.as_raw_fd()));
                        }
                    }
                    streams
                };
                for mut s in streams {
                    executor::spawn({
                        // TODO: could probably refcount this line. Maybe Bytes?
                        let line = line.clone();
                        let channel = channel.clone();
                        let submap = submap.clone();
                        async move {
                            let task_id = executor::get_task_id();
                            if let Err(e) = s.write_all(line.as_bytes()).await {
                                error!(
                                    "Failed to write line to channel={channel} fd={} task_id={task_id}: {e}",
                                    s.as_raw_fd()
                                );
                            }
                            submap.borrow_mut().get_mut(&channel).unwrap().sent += 1;
                        }
                    });
                }
            }
            Err(e) => {
                error!("Got error {e}");
                return;
            }
        }
    }
}

// After reading the subscribe message and sending OK, this task just keeps the subscriber alive until it leaves.
// The publisher is writing directly to the file descriptor which is shared by the shared map.
async fn handle_subscriber(
    mut reader: BufReader<unet::TcpStream>,
    mut writer: unet::TcpStream,
    channel: String,
    submap: Rc<RefCell<HashMap<String, SubscriberInfo>>>,
) {
    debug!(
        "Handling subscriber channel={channel} fd={}",
        writer.as_raw_fd()
    );
    let ok = b"OK\r\n";
    writer.write_all(ok).await.expect("OK");

    let new_stream_fd = writer.as_raw_fd();
    let new_stream = unet::TcpStream::new(writer.as_raw_fd());
    let _ = submap
        .borrow_mut()
        .entry(channel.clone())
        .or_default()
        .streams
        .insert(new_stream_fd, new_stream);

    loop {
        let mut line = String::new();
        match reader.read_line(&mut line).await {
            Ok(n) => {
                if n == 0 {
                    debug!("Subscriber left");
                    let _ = submap
                        .borrow_mut()
                        .get_mut(&channel)
                        .unwrap()
                        .streams
                        .remove(&new_stream_fd);
                    return;
                }
            }
            Err(e) => {
                error!("Got error {e}");
                return;
            }
        }
    }
}

fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();

    uring::init(uring::UringArgs {
        uring_size: args.uring_size,
        submissions_threshold: args.submissions_threshold,
        sqpoll_interval_ms: args.sqpoll_interval_ms,
    })?;

    executor::init();

    // Map of channel name to subscriber info
    let submap: Rc<RefCell<HashMap<String, SubscriberInfo>>> =
        Rc::new(RefCell::new(HashMap::new()));

    // General metrics routine for uring, executor, application metrics every 5s
    executor::spawn({
        let submap = submap.clone();
        async move {
            let mut timeout = Timeout::new(Duration::from_secs(5), true);
            loop {
                timeout = timeout.await.expect("REASON");
                let stats = uring::stats().expect("stats");
                info!("Uring metrics: {}", stats);

                // Print number of messages sent per subscriber so far
                for (channel, subs) in submap.borrow().iter() {
                    info!("{channel} sent {}", subs.sent);
                }
                for (_, subs) in submap.borrow_mut().iter_mut() {
                    subs.sent = 0;
                }
            }
        }
    });

    executor::spawn(async move {
        let mut listener = unet::TcpListener::bind("0.0.0.0:8080").unwrap();
        loop {
            debug!("Accepting");
            let stream = listener.accept_multi_fut().unwrap().await.unwrap();
            let submap = submap.clone();
            executor::spawn(async move {
                let task_id = executor::get_task_id();
                debug!("Got stream task_id={task_id} fd={}", stream.as_raw_fd());
                // TODO: idk, it's weird that I'm cloning the TcpStream. I could technically create
                // a second read against it but that would be bad...
                // Be careful!
                let fd = stream.as_raw_fd();
                let writer = unet::TcpStream::new(fd);
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                trace!(
                    "Reading protocol line fd={fd} task_id={}",
                    executor::get_task_id()
                );
                if let Err(e) = reader.read_line(&mut line).await {
                    error!("Failed to read line: {e}");
                    let mut stream = unet::TcpStream::new(fd);
                    stream.close().await.expect("Stream closing");
                    return;
                }
                trace!("Read protocol {line}");
                if line.starts_with("PUBLISH") {
                    let channel = line.trim()[8..].to_string();
                    handle_publisher(reader, writer, channel, submap).await;
                } else if line.starts_with("SUBSCRIBE") {
                    let channel = line.trim()[10..].to_string();
                    handle_subscriber(reader, writer, channel, submap).await;
                } else {
                    warn!(
                        "Line had length {} but didn't start with expected protocol fd={fd}",
                        line.len()
                    );
                }
                // TODO: Again, weird that I'm re-creating the tcp stream to close it but oh
                // wellsies
                let mut stream = unet::TcpStream::new(fd);
                if let Err(e) = stream.close().await {
                    warn!("Failed to close fd={}: {e}", stream.as_raw_fd());
                }
                debug!("Exiting task_id={task_id} fd={}", stream.as_raw_fd());
            });
        }
    });

    executor::run();

    Ok(())
}
