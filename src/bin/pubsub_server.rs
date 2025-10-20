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

fn send_error_and_then<F>(stream: unet::TcpStream, error_message: String, f: F) -> anyhow::Result<()>
    where F: FnOnce(i32) -> anyhow::Result<()> + 'static
{
    let error = BytesMut::with_capacity(1024);
    let mut writer = error.writer();
    let n = writer.write(format!("ERROR {error_message}\r\n").as_bytes())?;
    let error = writer.into_inner();
    stream.send(error.as_ptr(), n, move |res| {
        let _e = error;
        info!("Error result={res}");
        f(res)
    })
}

fn send_ok_and_then<F>(stream: unet::TcpStream, f: F) -> anyhow::Result<()>
    where F: FnOnce(i32) -> anyhow::Result<()> + 'static
{
    let mut ok = BytesMut::with_capacity(16);
    ok.put_slice(b"OK\r\n");
    stream.send(ok.as_ptr(), ok.len(), move |res| {
        let _o = ok;
        info!("Ok result res={res}");
        f(res)
    })
}

fn parse_channel(protocol: &BytesMut) -> Option<String> {
                    info!("P {}", protocol.len());
    for i in 0..protocol.len(){
        if protocol[i] == b'\n' && protocol[i-1] == b'\r' {
            let channel = std::str::from_utf8(&protocol[8..i-1]).expect("channel").to_string();
            return Some(channel);
        }
    }
    None
}

fn relay_messages(stream: unet::TcpStream) -> anyhow::Result<()> {
    Ok(())
}

fn do_protocol(stream: unet::TcpStream, mut protocol: BytesMut, res: i32) -> anyhow::Result<()> {
    let l = protocol.len();
    info!("Protocol res={res} l={l}");
    unsafe { protocol.set_len(l + res as usize) };
    let msg = std::str::from_utf8(&protocol[..]).unwrap();
    info!("Got msg {msg}");
    if protocol.starts_with(b"PUBLISH") {
        info!("Is Publisher");
        if let Some(channel) = parse_channel(&protocol) {
            info!("We got the channel {channel}");
            //send_ok_and_then(stream, move |res| {
            //    info!("Publisher sent ok channel={channel}");
            //    relay_messages(stream)
            //})?;
        } else {
            let ptr = protocol.as_mut_ptr().wrapping_add(l);
            let c = protocol.capacity() - l;
            //stream.recv(ptr, c, move |res| do_protocol(stream, protocol, res))?;
        }
    } else if protocol.starts_with(b"SUBSCRIBE") {
        send_ok_and_then(stream, move |res| {
            info!("Subscriber sent ok");
            Ok(())
        })?;
    } else {
        send_error_and_then(stream, "not a valid protocol".into(), |res| {
            info!("And then done");
            Ok(())
        })?;
    }

    Ok(())
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

    let listener = unet::TcpListener::bind("0.0.0.0:8080").expect("tcp listener");
    listener.accept(move |fd| {
        let stream = unet::TcpStream::new(fd);
        println!("Accepted");
        let protocol = BytesMut::with_capacity(1024);
        //stream.recv(protocol.as_mut_ptr(), protocol.capacity(), move |res| {
        //    do_protocol(stream, protocol, res)
        //})?;
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
    }, || {}) {
        error!("Error running uring: {}", e);
    }
    Ok(())
}
