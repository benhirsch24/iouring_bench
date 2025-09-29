use std::collections::HashMap;
use std::rc::Rc;

use log::{debug};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::runtime::Builder;
use tokio::task::{LocalSet, spawn_local};

static BUFFER_SIZE : usize = 1024*1024;
const HEALTH_OK: &'static str = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
const NOT_FOUND: &'static str = "HTTP/1.1 404 Not Found\r\nContent-Length: 8\r\n\r\nNotFound";
const BAD_REQUEST: &'static str = "HTTP/1.1 400 Bad Request\r\nContent-Length: 25\r\n\r\nExpected /object/<object>";
const SILLY_TEXT: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/assets/silly_text.txt"));

fn main() -> std::io::Result<()> {
    env_logger::init();
    let runtime = Builder::new_current_thread()
        .worker_threads(1)
        .thread_name("thready")
        .enable_io()
        .build()
        .unwrap();

    let local = LocalSet::new();
    runtime.block_on(local.run_until(async {
        let mut cache = Rc::new(HashMap::new());
        Rc::get_mut(&mut cache).unwrap().insert("1".to_string(), SILLY_TEXT.to_string());

        let listener = TcpListener::bind("0.0.0.0:8080").await?;
        loop {
            let (mut stream, _) = listener.accept().await?;
            let cache = cache.clone();
            spawn_local(async move {
                debug!("Accepted {:?}", stream);
                let mut buffer = vec![0u8; BUFFER_SIZE];
                let n = stream.read(&mut buffer).await.unwrap();

                let mut headers = [httparse::EMPTY_HEADER; 16];
                let mut r = httparse::Request::new(headers.as_mut_slice());
                let request = r.parse(&buffer[..n]).expect("parse error");

                // Not finished with request, keep reading
                if !request.is_complete() {
                    panic!("not complete");
                }

                // We have a request, let's route
                match r.path {
                    Some(path) => {
                        match path {
                            p if p.starts_with("/object") => {
                                let parts = p.split("/").collect::<Vec<_>>();
                                if parts.len() != 3 {
                                    stream.write(BAD_REQUEST.as_bytes()).await.unwrap();
                                } else {
                                    if let Some(o) = cache.get(&parts[2].to_string()) {
                                        stream.write_all(&format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}", o.len(), o).as_bytes()).await.unwrap();
                                    } else {
                                        stream.write(NOT_FOUND.as_bytes()).await.unwrap();
                                    }
                                }
                            },
                            "/health" => {
                                stream.write(HEALTH_OK.as_bytes()).await.unwrap();
                            },
                            _ => {
                                stream.write(NOT_FOUND.as_bytes()).await.unwrap();
                            }
                        }
                    },
                    None => {
                        stream.write(NOT_FOUND.as_bytes()).await.unwrap();
                    }
                }
            });
        }
        Ok(())
    }))
}
