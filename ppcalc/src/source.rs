use crate::trace;
use time::macros::datetime;

use ppcalc_metric::SourceId;

pub struct Source {
    number_of_messages: u64,
    inter_message_delay: time::Duration,
    start_offset: time::Duration,
}

impl Source {
    pub fn new(
        number_of_messages: u64,
        inter_message_delay: time::Duration,
        start_offset: time::Duration,
    ) -> Source {
        Source {
            number_of_messages,
            inter_message_delay,
            start_offset,
        }
    }
    pub fn gen_source_trace(&mut self, source_id: SourceId) -> trace::SourceTrace {
        let mut timestamps = vec![];
        let mut time = datetime!(1970-01-01 0:00) + self.start_offset;
        for _ in 0..self.number_of_messages {
            time = time.checked_add(self.inter_message_delay).unwrap();
            timestamps.push(time);
        }
        trace::SourceTrace {
            source_id: source_id,
            timestamps,
        }
    }
}
