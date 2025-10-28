use std::{cell::{Cell, UnsafeCell}, rc::Rc};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use crate::uring;

use log::{trace, warn};

struct ExecutorInner {
    results: HashMap<u64, i32>,
    multi_results: HashMap<u64, Vec<i32>>,
    tasks: HashMap<u64, Pin<Box<dyn Future<Output = ()>>>>,
    op_to_task: HashMap<u64, (u64, bool)>,
    next_task_id: u64,
    next_op_id: u64,
    ready_queue: Vec<u64>,

    timer_id: u64,
    timers: HashMap<u64, Box<io_uring::types::Timespec>>,
}

thread_local! {
    static THREAD_ID: Cell<u64> = Cell::new(0);
}

pub fn get_task_id() -> u64 {
    THREAD_ID.get()
}

fn set_task_id(new_task_id: u64) {
    THREAD_ID.set(new_task_id)
}

impl ExecutorInner {
    pub fn new() -> Self {
        Self {
            results: HashMap::new(),
            multi_results: HashMap::new(),
            tasks: HashMap::new(),
            op_to_task: HashMap::new(),
            next_task_id: 0,
            next_op_id: 0,
            ready_queue: Vec::new(),
            timer_id: 0,
            timers: HashMap::new(),
        }
    }

    fn register_timer(&mut self, ts: io_uring::types::Timespec) -> (u64, *const io_uring::types::Timespec) {
        let id = self.timer_id;
        self.timer_id += 1;
        let boxed = Box::new(ts);
        let ptr = &*boxed as *const io_uring::types::Timespec;
        self.timers.insert(id, boxed);
        (id, ptr)
    }

    pub fn unregister_timer(&mut self, timer_id: u64) {
        self.timers.remove(&timer_id);
    }

    fn handle_completion(&mut self, op: u64, res: i32, _flags: u32) -> Result<(), anyhow::Error> {
        if !self.op_to_task.contains_key(&op) {
            warn!("No op to task {op}");
            anyhow::bail!("No completion {op}");
        }

        let (task_id, is_multi)  = self.op_to_task.get(&op).copied().unwrap();
        trace!("handle_completion op={op} res={res} task_id={task_id} is_multi={is_multi}");
        self.ready_queue.push(task_id);
        if !is_multi {
            self.results.insert(op, res);
        } else {
            self.multi_results.entry(op).or_insert(Vec::new()).push(res);
        }
        Ok(())
    }

    fn handle_ready_queue(&mut self) {
        trace!("Ready queue len {}: {:?}", self.ready_queue.len(), self.ready_queue);
        let tasks: Vec<u64> = self.ready_queue.drain(..).collect();
        for task_id in tasks {
            set_task_id(task_id);
            trace!("Set task_id={task_id}");
            if let Some(mut task) = self.tasks.remove(&task_id) {
                let mut ctx = Context::from_waker(Waker::noop());
                match task.as_mut().poll(&mut ctx) {
                    Poll::Ready(_) => {
                        trace!("Task {task_id} complete");
                    },
                    Poll::Pending => {
                        trace!("Task still pending {task_id}");
                        self.tasks.insert(task_id, Box::pin(task));
                    },
                }
            }
        }
    }

    pub fn run(&mut self) {
        // Run the main uring loop using our callback
        uring::run(
            |op, res, flags| handle_completion(op, res, flags),
            || { handle_ready_queue() },
        ).expect("running uring");
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

    fn get_result(&mut self, op: u64) -> Option<i32> {
        if !self.op_to_task.contains_key(&op) {
            warn!("No task for op={op}");
            return None;
        }
        let (_, is_multi) = self.op_to_task.get(&op).unwrap();
        if *is_multi {
            if let Some(v) = self.multi_results.get_mut(&op) {
                v.pop()
            } else {
                None
            }
        } else {
            if let Some(res) = self.results.remove(&op) {
                trace!("Removed op={op}");
                self.op_to_task.remove(&op);
                Some(res)
            } else {
                None
            }
        }
    }

    fn spawn(&mut self, fut: impl Future<Output = ()> + 'static) {
        let task_id = self.get_next_task_id();
        self.tasks.insert(task_id, Box::pin(fut));
        self.ready_queue.push(task_id);
    }

    fn schedule_completion(&mut self, op: u64, is_multi: bool) {
        let task_id = get_task_id();
        self.op_to_task.insert(op, (task_id, is_multi));
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

pub fn spawn(fut: impl Future<Output = ()> + 'static) {
    EXECUTOR.with(|exe| {
        unsafe {
            let exe = &mut *exe.get();
            exe.as_mut().unwrap().spawn(Box::pin(fut));
        }
    })
}

pub fn register_timer(ts: io_uring::types::Timespec) -> (u64, *const io_uring::types::Timespec) {
    EXECUTOR.with(|exe| {
        unsafe {
            let exe = &mut *exe.get();
            exe.as_mut().unwrap().register_timer(ts)
        }
    })
}

pub fn unregister_timer(timer_id: u64) {
    EXECUTOR.with(|exe| {
        unsafe {
            let exe = &mut *exe.get();
            exe.as_mut().unwrap().unregister_timer(timer_id)
        }
    })
}

pub fn handle_completion(op: u64, res: i32, flags: u32) -> Result<(), anyhow::Error> {
    EXECUTOR.with(|exe| {
        unsafe {
            let exe = &mut *exe.get();
            exe.as_mut().unwrap().handle_completion(op, res, flags)
        }
    })
}

pub fn handle_ready_queue() {
    EXECUTOR.with(|exe| {
        unsafe {
            let exe = &mut *exe.get();
            exe.as_mut().unwrap().handle_ready_queue()
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

pub fn get_result(op_id: u64) -> Option<i32> {
    EXECUTOR.with(|exe| {
        unsafe {
            let exe = &mut *exe.get();
            exe.as_mut().unwrap().get_result(op_id)
        }
    })
}

pub fn schedule_completion(op_id: u64, is_multi: bool) {
    EXECUTOR.with(|exe| {
        unsafe {
            let exe = &mut *exe.get();
            exe.as_mut().unwrap().schedule_completion(op_id, is_multi)
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
        uring::init(uring::UringArgs::default()).expect("uring init!");
        Self {
            inner: Rc::new(UnsafeCell::new(ExecutorInner::new())),
        }
    }

    fn handle_completion(&mut self, op: u64, res: i32, flags: u32) -> Result<(), anyhow::Error> {
        unsafe {
            let inner = &mut *self.inner.get();
            inner.handle_completion(op, res, flags)
        }
    }

    fn spawn(&mut self, fut: impl Future<Output = ()> + 'static) {
        unsafe {
            let inner = &mut *self.inner.get();
            inner.spawn(Box::pin(fut));
        }
    }

    fn schedule_completion(&mut self, op: u64, is_multi: bool) {
        unsafe {
            let inner = &mut *self.inner.get();
            inner.schedule_completion(op, is_multi);
        }
    }

    fn get_result(&self, op: u64) -> Option<i32> {
        unsafe {
            let inner = &mut *self.inner.get();
            inner.get_result(op)
        }
    }

    pub fn run(&mut self) {
        unsafe {
            let inner = &mut *self.inner.get();
            inner.run()
        }
    }
}

mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
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
        let f = {
            let mut executor = executor.clone();
            let task_id = task_id;
            let res = res.clone();
            async move {
                let example_future_op = 7;
                let fut = Box::pin(ExampleFuture { id: example_future_op, executor: executor.clone() });
                executor.schedule_completion(example_future_op);
                fut.await;
                *res.borrow_mut() = true;
                ()
            }
        };
        let id: u64 = 5;
        executor.spawn(task_id, f);
        executor.schedule_completion(id);
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
