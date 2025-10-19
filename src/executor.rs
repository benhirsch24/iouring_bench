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
    op_to_task: HashMap<u64, u64>,
    next_task_id: u64,
    next_op_id: u64,
}

impl ExecutorInner {
    pub fn new() -> Self {
        Self {
            results: HashMap::new(),
            tasks: HashMap::new(),
            op_to_task: HashMap::new(),
            next_task_id: 0,
            next_op_id: 0,
        }
    }

    fn handle_completion(&mut self, op: u64, res: i32, flags: u32) -> Result<(), anyhow::Error> {
        if !self.op_to_task.contains_key(&op) {
            anyhow::bail!("No completion {op}");
        }

        let task_id  = self.op_to_task.get(&op).copied().unwrap();
        self.results.insert(op, res);
        if self.tasks.contains_key(&task_id) {
            let mut task = self.tasks.remove(&task_id).expect("We just verified");
            let mut ctx = Context::from_waker(Waker::noop());
            match task.as_mut().poll(&mut ctx) {
                Poll::Ready(_) => {
                    info!("Task complete");
                    self.results.remove(&op);
                },
                Poll::Pending => {
                    self.tasks.insert(task_id, Box::pin(task));
                },
            }
        }
        Ok(())
    }

    pub fn run(&mut self) {
        // Handle task0 first
        // TODO: I probably do need to evolve this into:
        // 1. Handle all completions (push ready tasks to ready queue)
        // 2. Poll ready tasks
        //
        // That way I can spawn arbitrary tasks eg timeouts which can be polled after the next
        // completion queue run. But I'll defer that for a little bit
        // This block below polls the first task
        let op = self.get_next_op_id();
        self.schedule_completion(0, op);
        self.handle_completion(0, 0, 0);

        // Run the main uring loop using our callback
        uring::run(|op, res, flags| self.handle_completion(op, res, flags)).expect("running uring");
    }

    fn get_next_op_id(&mut self) -> u64 {
        let op = self.next_op_id;
        self.next_op_id += 1;
        op
    }

    fn get_next_task_id(&mut self) -> u64 {
        let task = self.next_task_id;
        self.next_task_id += 1;
        task
    }

    fn get_result(&self, op: u64) -> Option<i32> {
        self.results.get(&op).copied()
    }

    fn schedule(&mut self, task_id: u64, fut: Pin<Box<dyn Future<Output = ()>>>) {
        self.tasks.insert(task_id, fut);
    }

    fn schedule_completion(&mut self, task_id: u64, op: u64) {
        self.op_to_task.insert(op, task_id);
    }
}

thread_local! {
    static EXECUTOR: UnsafeCell<Option<ExecutorInner>> = UnsafeCell::new(None);
}

pub fn init() {
    EXECUTOR.with(|exe| {
        unsafe {
            let exe = &mut *exe.get();
            if exe.is_some() {
                return;
            }

            let new_exe = ExecutorInner::new();
            *exe = Some(new_exe);
        }
    })
}

pub fn schedule(task_id: u64, fut: Pin<Box<dyn Future<Output = ()>>>) {
    EXECUTOR.with(|exe| {
        unsafe {
            let exe = &mut *exe.get();
            exe.as_mut().unwrap().schedule(task_id, fut);
        }
    })
}

pub fn get_next_op_id() -> u64 {
    EXECUTOR.with(|exe| {
        unsafe {
            let exe = &mut *exe.get();
            exe.as_mut().unwrap().get_next_op_id()
        }
    })
}

pub fn get_next_task_id() -> u64 {
    EXECUTOR.with(|exe| {
        unsafe {
            let exe = &mut *exe.get();
            exe.as_mut().unwrap().get_next_task_id()
        }
    })
}

pub fn get_result(op_id: u64) -> Option<i32> {
    EXECUTOR.with(|exe| {
        unsafe {
            let exe = &mut *exe.get();
            exe.as_mut().unwrap().get_result(op_id)
        }
    })
}

pub fn schedule_completion(task_id: u64, op_id: u64) {
    EXECUTOR.with(|exe| {
        unsafe {
            let exe = &mut *exe.get();
            exe.as_mut().unwrap().schedule_completion(task_id, op_id)
        }
    })
}

pub fn run() {
    EXECUTOR.with(|exe| {
        unsafe {
            let exe = &mut *exe.get();
            exe.as_mut().unwrap().run()
        }
    })
}

#[derive(Clone)]
struct Executor {
    inner: Rc<UnsafeCell<ExecutorInner>>,
}

impl Executor {
    pub fn new() -> Self {
        // If uring was already initialized this will be a no-op
        uring::init(uring::UringArgs::default());
        Self {
            inner: Rc::new(UnsafeCell::new(ExecutorInner::new())),
        }
    }

    fn handle_completion(&mut self, op: u64, res: i32, flags: u32) -> Result<(), anyhow::Error> {
        unsafe {
            let mut inner = &mut *self.inner.get();
            inner.handle_completion(op, res, flags)
        }
    }

    fn schedule(&mut self, op: u64, fut: Pin<Box<dyn Future<Output = ()>>>) {
        unsafe {
            let mut inner = &mut *self.inner.get();
            inner.schedule(op, fut);
        }
    }

    fn schedule_completion(&mut self, task_id: u64, op: u64) {
        unsafe {
            let mut inner = &mut *self.inner.get();
            inner.schedule_completion(task_id, op);
        }
    }

    fn get_result(&self, op: u64) -> Option<i32> {
        unsafe {
            let mut inner = &mut *self.inner.get();
            inner.get_result(op)
        }
    }

    pub fn run(&mut self) {
        unsafe {
            let mut inner = &mut *self.inner.get();
            inner.run()
        }
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
                let example_future_op = 7;
                let fut = Box::pin(ExampleFuture { id: example_future_op, executor: executor.clone() });
                executor.schedule_completion(task_id, example_future_op);
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
