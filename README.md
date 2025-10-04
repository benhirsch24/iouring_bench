# IO Uring Benchmarks

Some very basic benchmarking with a couple of different technologies, mainly focused around seeing how io_uring stacks up.

* `src/main.rs` - io_uring based server. Run with `RUST_LOG=error ./iouring_bench -h`
* `src/bin/tokio.rs` - [tokio](https://docs.rs/tokio/latest/tokio/) based server.
* `src/bin/glommio.rs` - [glommio](https://docs.rs/glommio/latest/glommio/) based server

At the moment each program simply serves a static file in `assets/silly_text.txt`.

I've tested using [vegeta](https://pkg.go.dev/github.com/tsenart/vegeta/v12) using something like

`echo "GET http://<IP>:8080/object/1" | ./vegeta attack -duration=60s -rate=1000/1s | tee results.bin | ./vegeta report`

And mostly tested on EC2 c7g.8xlarge for now, with plans to go bigger.
