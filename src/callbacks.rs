use crate::uring;
use io_uring::squeue::Entry;
use std::cell::UnsafeCell;
use std::collections::HashMap;

struct CallbackRegistry {
    map: HashMap<u64, Box<dyn FnOnce(i32) -> anyhow::Result<()>>>,
    counter: u64,
}

impl CallbackRegistry {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            // TODO: Reserving special user data 0 for the metrics timeout (for now, until I
            // write a more scalable solution
            counter: 1,
        }
    }

    fn add_callback<T>(&mut self, f: T) -> u64
    where
        T: FnOnce(i32) -> anyhow::Result<()> + 'static,
    {
        let c = self.counter;
        self.map.insert(c, Box::new(f));
        self.counter += 1;
        c
    }

    fn call_back(&mut self, id: u64, res: i32) -> anyhow::Result<()> {
        let cb = self.map.remove(&id);
        match cb {
            Some(cb) => cb(res),
            None => anyhow::bail!("No callback registered for {id} res={res}"),
        }
    }
}

thread_local! {
    static CB: UnsafeCell<CallbackRegistry> = UnsafeCell::new(CallbackRegistry::new());
}

pub fn add_callback<F>(f: F) -> u64
where
    F: FnOnce(i32) -> anyhow::Result<()> + 'static,
{
    CB.with(|cbr| unsafe {
        let cb_ref = &mut *cbr.get();
        cb_ref.add_callback(Box::new(f))
    })
}

pub fn call_back(id: u64, res: i32) -> anyhow::Result<()> {
    CB.with(|cbr| unsafe {
        let cb_ref = &mut *cbr.get();
        cb_ref.call_back(id, res)
    })
}

pub fn submit_entry<F>(entry: Entry, f: F) -> anyhow::Result<()>
where
    F: FnOnce(i32) -> anyhow::Result<()> + 'static,
{
    let ud = add_callback(Box::new(f));
    uring::submit(entry.user_data(ud))?;
    Ok(())
}
