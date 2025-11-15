use clap::Parser;
use futures::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use log::{debug, error, info, trace, warn};

use std::cell::Cell;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::os::fd::RawFd;
use std::time::Duration;
use std::{cell::RefCell, rc::Rc};

use iouring_bench::executor;
use iouring_bench::net as unet;
use iouring_bench::sync::mpsc;
use iouring_bench::timeout::TimeoutFuture as Timeout;
use iouring_bench::uring;

static OK: &[u8] = b"OK\r\n";

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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ChannelId(u32);

impl ChannelId {
    fn allocate(generator: &Rc<Cell<u32>>) -> Self {
        let next = generator.get();
        generator.set(next + 1);
        ChannelId(next)
    }
}

struct SubscriberInfo {
    id: ChannelId,
    senders: HashMap<RawFd, mpsc::Sender<Rc<String>>>,
}

#[derive(Clone)]
enum StatEvent {
    ChannelRegistered { channel_id: ChannelId, name: String },
    ChannelRemoved { channel_id: ChannelId },
    MessagesSent { channel_id: ChannelId, count: u64 },
}

struct ChannelStats {
    name: String,
    sent: u64,
    active: bool,
}

fn remove_subscriber(
    submap: &Rc<RefCell<HashMap<String, SubscriberInfo>>>,
    channel: &str,
    subscriber_fd: RawFd,
    stats_tx: &mut mpsc::Sender<StatEvent>,
) {
    let mut map = submap.borrow_mut();
    let remove_channel = match map.get_mut(channel) {
        Some(info) => {
            info.senders.remove(&subscriber_fd);
            if info.senders.is_empty() {
                Some(info.id)
            } else {
                None
            }
        }
        None => None,
    };
    if let Some(channel_id) = remove_channel {
        map.remove(channel);
        let _ = stats_tx.send(StatEvent::ChannelRemoved { channel_id });
    }
}

async fn handle_publisher(
    mut reader: BufReader<unet::TcpStream>,
    mut writer: unet::TcpStream,
    channel: String,
    submap: Rc<RefCell<HashMap<String, SubscriberInfo>>>,
    mut stats_tx: mpsc::Sender<StatEvent>,
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
                    if let Some(info) = submap.borrow().get(&channel) {
                        let _ = stats_tx.send(StatEvent::MessagesSent {
                            channel_id: info.id,
                            count: delivered,
                        });
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
    channel_ids: Rc<Cell<u32>>,
    mut stats_tx: mpsc::Sender<StatEvent>,
) {
    debug!(
        "Handling subscriber channel={channel} fd={}",
        writer.as_raw_fd()
    );
    writer.write_all(OK).await.expect("OK");

    let subscriber_fd = writer.as_raw_fd();
    let (rx_inner, tx) = mpsc::channel::<Rc<String>>();
    let rx = Rc::new(RefCell::new(rx_inner));
    {
        let mut map = submap.borrow_mut();
        match map.entry(channel.clone()) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().senders.insert(subscriber_fd, tx);
            }
            Entry::Vacant(entry) => {
                let channel_id = ChannelId::allocate(&channel_ids);
                entry
                    .insert(SubscriberInfo {
                        id: channel_id,
                        senders: HashMap::new(),
                    })
                    .senders
                    .insert(subscriber_fd, tx);
                let _ = stats_tx.send(StatEvent::ChannelRegistered {
                    channel_id,
                    name: channel.clone(),
                });
            }
        }
    }

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
                    remove_subscriber(&submap, &channel, subscriber_fd, &mut stats_tx);
                    rx.borrow_mut().close();
                    return;
                }
            }
            Err(e) => {
                error!("Got error {e}");
                remove_subscriber(&submap, &channel, subscriber_fd, &mut stats_tx);
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
    let channel_ids = Rc::new(Cell::new(0u32));

    let stats_map: Rc<RefCell<HashMap<ChannelId, ChannelStats>>> =
        Rc::new(RefCell::new(HashMap::new()));
    let (stats_rx, stats_tx) = mpsc::channel::<StatEvent>();

    executor::spawn({
        let stats_map = stats_map.clone();
        let mut stats_rx = stats_rx;
        async move {
            loop {
                match stats_rx.recv().await {
                    Some(StatEvent::ChannelRegistered { channel_id, name }) => {
                        stats_map.borrow_mut().insert(
                            channel_id,
                            ChannelStats {
                                name,
                                sent: 0,
                                active: true,
                            },
                        );
                    }
                    Some(StatEvent::ChannelRemoved { channel_id }) => {
                        if let Some(entry) = stats_map.borrow_mut().get_mut(&channel_id) {
                            entry.active = false;
                        }
                    }
                    Some(StatEvent::MessagesSent { channel_id, count }) => {
                        if let Some(entry) = stats_map.borrow_mut().get_mut(&channel_id) {
                            entry.sent += count;
                        }
                    }
                    None => break,
                }
            }
        }
    });

    // General metrics routine for uring, executor, application metrics every 5s
    executor::spawn({
        let stats_map = stats_map.clone();
        async move {
            let mut timeout = Timeout::new(Duration::from_secs(5), true);
            loop {
                timeout = timeout.await.expect("REASON");
                let stats = uring::stats().expect("stats");
                info!("Uring metrics: {}", stats);

                // Print number of messages sent per subscriber so far
                let mut to_remove = Vec::new();
                {
                    let mut map = stats_map.borrow_mut();
                    for (channel_id, entry) in map.iter_mut() {
                        if entry.active || entry.sent > 0 {
                            info!("{} sent {}", entry.name, entry.sent);
                        }
                        let remove_entry = !entry.active && entry.sent == 0;
                        entry.sent = 0;
                        if remove_entry {
                            to_remove.push(*channel_id);
                        }
                    }
                }
                if !to_remove.is_empty() {
                    let mut map = stats_map.borrow_mut();
                    for channel_id in to_remove {
                        map.remove(&channel_id);
                    }
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
            let channel_ids = channel_ids.clone();
            let stats_tx = stats_tx.clone();
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
                    handle_publisher(reader, writer, channel, submap, stats_tx).await;
                } else if line.starts_with("SUBSCRIBE") {
                    let channel = line.trim()[10..].to_string();
                    handle_subscriber(reader, writer, channel, submap, channel_ids, stats_tx).await;
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
