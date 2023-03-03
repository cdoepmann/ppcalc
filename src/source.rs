use crate::trace;
use rand::distributions::{Distribution, Uniform};
use rand::prelude::*;
use time::macros::datetime;
use time::Duration;

pub struct Source {
    number_of_messages: u64,
    rng: ThreadRng,
    distr: Uniform<i64>,
}

impl Source {
    pub fn new(number_of_messages: u64, min: i64, max: i64) -> Source {
        Source {
            number_of_messages,
            distr: Uniform::from(min..max),
            rng: rand::thread_rng(),
        }
    }
    pub fn gen_source_trace(&mut self, source_name: String) -> trace::SourceTrace {
        let mut timestamps = vec![];
        let mut time = datetime!(1970-01-01 0:00);
        for _ in 0..self.number_of_messages {
            let offset: time::Duration =
                time::Duration::milliseconds(self.distr.sample(&mut self.rng));
            time = time.checked_add(Duration::from(offset)).unwrap();
            timestamps.push(time);
        }
        trace::SourceTrace {
            source_name,
            timestamps,
        }
    }
}
