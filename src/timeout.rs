use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use io_uring::{opcode, types};
use log::trace;

use crate::executor;
use crate::uring;

pub struct TimeoutFuture {
    op_id: u64,
    timer_id: u64,
    repeated: bool,
    done: bool,
}

impl TimeoutFuture {
    pub fn new(dur: Duration, repeated: bool) -> Self {
        let ts = types::Timespec::new().sec(dur.as_secs()).nsec(dur.subsec_nanos());
        let (timer_id, ts) = executor::register_timer(ts);
        let op_id = executor::get_next_op_id();
        let count = if repeated { 0 } else { 1 };
        let timeout = opcode::Timeout::new(ts)
            .flags(types::TimeoutFlags::MULTISHOT)
            .count(count)
            .build()
            .user_data(op_id);
        trace!("Scheduling {dur:?} {repeated} timeout op={op_id}");
        executor::schedule_completion(op_id, true);
        uring::submit(timeout).expect("arm timeout");
        Self {
            op_id,
            repeated,
            timer_id,
            done: false,
        }
    }

    pub fn get_id(&self) -> u64 {
        self.op_id
    }
}

impl Future for TimeoutFuture {
    type Output = std::io::Result<TimeoutFuture>;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.as_ref();
        if me.done {
            return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, "Timer already expired")));
        }
        match executor::get_result(me.op_id) {
            Some(res) => {
                if !me.repeated {
                    executor::unregister_timer(me.timer_id);
                }
                if res == -62 {
                    Poll::Ready(Ok(TimeoutFuture {
                        op_id: me.op_id,
                        timer_id: me.timer_id,
                        repeated: me.repeated,
                        done: !me.repeated,
                    }))
                } else {
                    Poll::Ready(Err(std::io::Error::from_raw_os_error(-res)))
                }
            },
            None => {
                Poll::Pending
            }
        }
    }
}
