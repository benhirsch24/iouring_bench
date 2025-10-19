use std::{cell::{RefCell, UnsafeCell}, rc::Rc};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use crate::uring;

use log::info;

struct ExecutorInner {
    results: HashMap<u64, i32>,
    tasks: HashMap<u64, Pin<Box<dyn Future<Output = ()>>>>,
    ud_to_task: HashMap<u64, u64>,
}

impl ExecutorInner {
    pub fn new() -> Self {
        Self {
            results: HashMap::new(),
            tasks: HashMap::new(),
            ud_to_task: HashMap::new(),
        }
    }

    fn handle_completion(&mut self, ud: u64, res: i32, flags: u32) -> Result<(), anyhow::Error> {
        if !self.ud_to_task.contains_key(&ud) {
            anyhow::bail!("No completion {ud}");
        }

        let task_id  = self.ud_to_task.get(&ud).copied().unwrap();
        self.results.insert(ud, res);
        if self.tasks.contains_key(&task_id) {
            let mut task = self.tasks.remove(&task_id).expect("We just verified");
            let mut ctx = Context::from_waker(Waker::noop());
            match task.as_mut().poll(&mut ctx) {
                Poll::Ready(_) => {
                    info!("Task complete");
                    self.results.remove(&ud);
                },
                Poll::Pending => {
                    self.tasks.insert(task_id, Box::pin(task));
                },
            }
        }
        Ok(())
    }

    fn get_result(&self, ud: u64) -> Option<&i32> {
        self.results.get(&ud)
    }

    fn schedule(&mut self, ud: u64, fut: Pin<Box<dyn Future<Output = ()>>>) {
        self.tasks.insert(ud, fut);
    }

    fn schedule_completion(&mut self, task_id: u64, ud: u64) {
        self.ud_to_task.insert(ud, task_id);
    }
}

#[derive(Clone)]
struct Executor {
    inner: Rc<UnsafeCell<ExecutorInner>>,
}

impl Executor {
    pub fn new() -> Self {
        Self {
            inner: Rc::new(UnsafeCell::new(ExecutorInner::new())),
        }
    }

    fn handle_completion(&mut self, ud: u64, res: i32, flags: u32) -> Result<(), anyhow::Error> {
        unsafe {
            let mut inner = &mut *self.inner.get();
            inner.handle_completion(ud, res, flags)
        }
    }

    fn schedule(&mut self, ud: u64, fut: Pin<Box<dyn Future<Output = ()>>>) {
        unsafe {
            let mut inner = &mut *self.inner.get();
            inner.schedule(ud, fut);
        }
    }

    fn schedule_completion(&mut self, task_id: u64, ud: u64) {
        unsafe {
            let mut inner = &mut *self.inner.get();
            inner.schedule_completion(task_id, ud);
        }
    }

    fn get_result(&self, ud: u64) -> Option<i32> {
        unsafe {
            let mut inner = &mut *self.inner.get();
            inner.get_result(ud).copied()
        }
    }

    pub fn run(&mut self) {
        // Run the main uring loop using our callback
        uring::run(|ud, res, flags| self.handle_completion(ud, res, flags)).expect("running uring");
    }
}

mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};
    use std::{cell::RefCell, rc::Rc};
    use super::Executor;

    struct ExampleFuture {
        id: u64,
        executor: Executor,
    }

    impl Future for ExampleFuture {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            let me = self.as_ref();
            match me.executor.get_result(me.id) {
                Some(res) => {
                    println!("Got result {res}");
                    Poll::Ready(())
                },
                None => {
                    println!("no result yet");
                    Poll::Pending
                }
            }
        }
    }

    #[test]
    fn test1() {
        let mut executor = Executor::new();
        let task_id = 1;
        let res = Rc::new(RefCell::new(false));
        let f = Box::pin({
            let mut executor = executor.clone();
            let task_id = task_id;
            let res = res.clone();
            async move {
                let example_future_ud = 7;
                let fut = Box::pin(ExampleFuture { id: example_future_ud, executor: executor.clone() });
                executor.schedule_completion(task_id, example_future_ud);
                fut.await;
                *res.borrow_mut() = true;
                ()
            }
        });
        let id: u64 = 5;
        executor.schedule(task_id, f);
        executor.schedule_completion(task_id, id);
        executor.handle_completion(5, 0, 0).expect("No error");
        if let Ok(_) = executor.handle_completion(6, 0, 0) {
            panic!("No scheduled completion 6");
        }
        executor.handle_completion(7, 0, 0).expect("No error");
        if !*res.borrow() {
            panic!("res should be true");
        }
    }
}
