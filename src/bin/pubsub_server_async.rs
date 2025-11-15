use clap::Parser;
use futures::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use log::{debug, error, info, trace, warn};

use std::collections::HashMap;
use std::os::fd::RawFd;
use std::time::Duration;
use std::{cell::RefCell, rc::Rc};

use iouring_bench::executor;
use iouring_bench::net as unet;
use iouring_bench::sync::mpsc;
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
    senders: HashMap<RawFd, mpsc::Sender<Rc<String>>>,
    sent: u64,
}

static OK: &[u8] = b"OK\r\n";

async fn handle_publisher(
    mut reader: BufReader<unet::TcpStream>,
    mut writer: unet::TcpStream,
    channel: String,
    submap: Rc<RefCell<HashMap<String, SubscriberInfo>>>,
) {
    let task_id = executor::get_task_id();
    let fd = writer.as_raw_fd();
    debug!("Handling publisher channel={channel} fd={fd} task_id={task_id}");
    writer.write_all(OK).await.expect("OK");
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
                let senders = {
                    let mut senders = Vec::new();
                    if let Some(set) = submap.borrow().get(&channel) {
                        for sender in set.senders.values() {
                            senders.push(sender.clone());
                        }
                    }
                    senders
                };
                let payload = Rc::new(line.clone());
                let mut delivered = 0u64;
                for mut sender in senders {
                    match sender.send(payload.clone()) {
                        Ok(()) => delivered += 1,
                        Err(mpsc::SendError::Closed) => {
                            debug!("Sender closed for channel={channel}");
                        }
                        Err(mpsc::SendError::Full) => unreachable!("Channel is unbounded"),
                    }
                }
                if delivered > 0 {
                    if let Some(info) = submap.borrow_mut().get_mut(&channel) {
                        info.sent += delivered;
                    }
                }
            }
            Err(e) => {
                error!("Got error {e}");
                return;
            }
        }
    }
}

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
    writer.write_all(OK).await.expect("OK");

    let subscriber_fd = writer.as_raw_fd();
    let (rx_inner, tx) = mpsc::channel::<Rc<String>>();
    let rx = Rc::new(RefCell::new(rx_inner));
    let _ = submap
        .borrow_mut()
        .entry(channel.clone())
        .or_default()
        .senders
        .insert(subscriber_fd, tx);

    executor::spawn({
        let rx = rx.clone();
        let mut writer = writer;
        let channel = channel.clone();
        async move {
            let task_id = executor::get_task_id();
            let fd = writer.as_raw_fd();
            loop {
                let fut = { rx.borrow_mut().recv() };
                match fut.await {
                    Some(msg) => {
                        if let Err(e) = writer.write_all(msg.as_bytes()).await {
                            error!(
                                "Failed to write line to channel={channel} fd={fd} task_id={task_id}: {e}"
                            );
                            break;
                        }
                    }
                    None => {
                        debug!("Writer exiting channel={channel} fd={fd} task_id={task_id}");
                        break;
                    }
                }
            }
            if let Err(e) = writer.close().await {
                warn!("Failed to close writer fd={fd} channel={channel}: {e}");
            }
        }
    });

    loop {
        let mut line = String::new();
        match reader.read_line(&mut line).await {
            Ok(n) => {
                if n == 0 {
                    debug!("Subscriber left");
                    if let Some(info) = submap.borrow_mut().get_mut(&channel) {
                        info.senders.remove(&subscriber_fd);
                    }
                    rx.borrow_mut().close();
                    return;
                }
            }
            Err(e) => {
                error!("Got error {e}");
                if let Some(info) = submap.borrow_mut().get_mut(&channel) {
                    info.senders.remove(&subscriber_fd);
                }
                rx.borrow_mut().close();
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
