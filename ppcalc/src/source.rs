use crate::trace;
use num_traits::cast::ToPrimitive;
use rand::prelude::*;
use time::macros::datetime;
use time::Duration;

use ppcalc_metric::SourceId;

pub struct Source<T: Distribution<f64>> {
    number_of_messages: u64,
    rng: ThreadRng,
    sending_distr: T,
    start_distr: T,
}

impl<T: Distribution<f64>> Source<T> {
    pub fn new(number_of_messages: u64, distr: T, start_distr: T) -> Source<T> {
        Source {
            number_of_messages,
            sending_distr: distr,
            start_distr: start_distr,
            rng: rand::thread_rng(),
        }
    }
    pub fn gen_source_trace(&mut self, source_id: SourceId) -> trace::SourceTrace {
        let mut timestamps = vec![];
        let mut time = datetime!(1970-01-01 0:00)
            + time::Duration::milliseconds(
                self.start_distr.sample(&mut self.rng).to_i64().unwrap(),
            );
        for _ in 0..self.number_of_messages {
            let offset: time::Duration = time::Duration::milliseconds(
                self.sending_distr.sample(&mut self.rng).to_i64().unwrap(),
            );
            time = time.checked_add(Duration::from(offset)).unwrap();
            timestamps.push(time);
        }
        trace::SourceTrace {
            source_id: source_id,
            timestamps,
        }
    }
}
