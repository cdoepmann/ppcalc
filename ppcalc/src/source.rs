use crate::trace;
use num_traits::cast::ToPrimitive;
use rand::prelude::*;
use time::macros::datetime;
use time::Duration;

use crate::cli::ParsedDistribution;

use ppcalc_metric::SourceId;

pub struct Source {
    number_of_messages: u64,
    rng: ThreadRng,
    sending_distr: ParsedDistribution<f64>,
    start_distr: ParsedDistribution<f64>,
}

impl Source {
    pub fn new(
        number_of_messages: u64,
        distr: ParsedDistribution<f64>,
        start_distr: ParsedDistribution<f64>,
    ) -> Source {
        Source {
            number_of_messages,
            sending_distr: distr,
            start_distr: start_distr,
            rng: rand::thread_rng(),
        }
    }
    pub fn gen_source_trace(&mut self, source_id: SourceId) -> trace::SourceTrace {
        let start_distr = self.start_distr.make_distr().unwrap();
        let sending_distr = self.sending_distr.make_distr().unwrap();

        let mut timestamps = vec![];
        let mut time = datetime!(1970-01-01 0:00)
            + time::Duration::milliseconds(start_distr.sample(&mut self.rng).to_i64().unwrap());
        for _ in 0..self.number_of_messages {
            let offset: time::Duration =
                time::Duration::milliseconds(sending_distr.sample(&mut self.rng).to_i64().unwrap());
            time = time.checked_add(Duration::from(offset)).unwrap();
            timestamps.push(time);
        }
        trace::SourceTrace {
            source_id: source_id,
            timestamps,
        }
    }
}
