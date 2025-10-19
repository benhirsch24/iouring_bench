struct Runtime {
}

impl Runtime {
    pub fn new() -> Self {
        Self {}
    }

    pub fn run() -> anyhow::Result<()> {
        uring::run(move |ud, res, _flags| {
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
        })
    }
}
