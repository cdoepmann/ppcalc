use crate::trace;
use num_traits::cast::ToPrimitive;
use rand::prelude::*;
use time::macros::datetime;
use time::Duration;

pub struct Source<T: Distribution<f64>> {
    number_of_messages: u64,
    rng: ThreadRng,
    distr: T,
}

impl<T: Distribution<f64>> Source<T> {
    pub fn new(number_of_messages: u64, distr: T) -> Source<T> {
        Source {
            number_of_messages,
            distr: distr,
            rng: rand::thread_rng(),
        }
    }
    pub fn gen_source_trace(&mut self, source_name: String) -> trace::SourceTrace {
        let mut timestamps = vec![];
        let mut time = datetime!(1970-01-01 0:00);
        for _ in 0..self.number_of_messages {
            let offset: time::Duration =
                time::Duration::milliseconds(self.distr.sample(&mut self.rng).to_i64().unwrap());
            time = time.checked_add(Duration::from(offset)).unwrap();
            timestamps.push(time);
        }
        trace::SourceTrace {
            source_name,
            timestamps,
        }
    }
}
