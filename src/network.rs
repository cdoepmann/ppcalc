use crate::trace;
use rand::{distributions::Uniform, prelude::Distribution};
use std::collections::HashMap;

// It is important that this is (to some extend) reproducable, so we can change/analyse the destination distribution!
// Lets maybe only create the entries we need?
pub fn generate_network_delay(
    min_delay: i64,
    max_delay: i64,
    pre_network_trace: Vec<trace::PreNetworkTraceEntry>,
) -> trace::Trace {
    let mut trace = vec![];
    let mut m_id = 0;
    let distr = Uniform::from(min_delay..max_delay);
    let mut rng = rand::thread_rng();
    let delay = distr.sample(&mut rng);
    for entry in pre_network_trace {
        trace.push(trace::TraceEntry {
            m_id: m_id,
            source_id: entry.source_id,
            source_timestamp: entry.source_timestamp,
            destination_id: entry.destination_id,
            destination_timestamp: entry
                .source_timestamp
                .checked_add(time::Duration::from(time::Duration::milliseconds(delay)))
                .unwrap(),
        });
        m_id += 1;
    }
    trace::Trace { entries: trace }
}

/* Todo we have sorted vectors of timestamps, this should be doable in something like timestamps * log(sources) */
pub fn merge_traces(
    source_traces: Vec<trace::SourceTrace>,
    source_destination_map: &HashMap<u64, u64>,
) -> Vec<trace::PreNetworkTraceEntry> {
    let mut pre_network_trace = vec![];
    for trace in source_traces {
        let destination_id = source_destination_map.get(&trace.source_id).unwrap();
        for ts in trace.timestamps {
            pre_network_trace.push(trace::PreNetworkTraceEntry {
                source_id: trace.source_id,
                source_timestamp: ts,
                destination_id: *destination_id,
            });
        }
    }
    pre_network_trace.sort_by(|a, b| a.source_timestamp.cmp(&b.source_timestamp));
    pre_network_trace
}
