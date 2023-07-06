use fxhash::FxHashMap as HashMap;
use fxhash::FxHashSet as HashSet;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::{min, Ordering};
use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::{collections::hash_map::Entry, fmt::Display, fs::File, io::BufReader, ops::Add};
use time::{Duration, PrimitiveDateTime};

use crate::bench;
use crate::trace::{DestinationId, MessageId, SourceId, Trace};

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord)]
enum EventTypeAndId {
    AddSourceMessage(SourceId),
    AddDestinationMessage(DestinationId),
    RemoveSourceMessage(SourceId),
}

impl EventTypeAndId {
    fn id_to_string(&self) -> String {
        match self {
            EventTypeAndId::AddDestinationMessage(dest_id) => dest_id.to_string(),
            EventTypeAndId::AddSourceMessage(source_id) => source_id.to_string(),
            EventTypeAndId::RemoveSourceMessage(source_id) => source_id.to_string(),
        }
    }
}

impl Display for EventTypeAndId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventTypeAndId::AddDestinationMessage(_) => f.write_str("AddDestinationMessage"),
            EventTypeAndId::AddSourceMessage(_) => f.write_str("AddSourceMessage"),
            EventTypeAndId::RemoveSourceMessage(_) => f.write_str("RemoveSourceMessage"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Ord)]
struct ProcessingEvent {
    event_type: EventTypeAndId,
    ts: PrimitiveDateTime,
    m_id: MessageId,
}
impl Display for ProcessingEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} {} {}",
            self.event_type,
            self.ts,
            self.m_id,
            self.event_type.id_to_string()
        )
    }
}

impl PartialOrd for ProcessingEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.ts.eq(&other.ts) {
            return self.event_type.partial_cmp(&other.event_type);
        }
        Some(self.ts.cmp(&other.ts))
    }
}

/// Compute the relative difference between two message anonymity sets.
///
/// Returns a pair of `usize`s, with the following meaning:
/// 1. the number of _newly added_ messages from `set1` to `set2`
/// 2. the number of _shared_ messages (overlap) between the two sets
fn relative_set_distance(set1: &HashSet<MessageId>, set2: &HashSet<MessageId>) -> (usize, usize) {
    (
        set2.difference(&set1).count(),
        set2.intersection(&set1).count(),
    )
}

/// Split an anonymity set by the destination of its messages
fn split_by_destination(
    set: HashSet<MessageId>,
    destination_mapping: &HashMap<MessageId, DestinationId>,
) -> HashMap<DestinationId, HashSet<MessageId>> {
    let mut res: HashMap<DestinationId, HashSet<MessageId>> = HashMap::default();

    for message in set {
        let destination = destination_mapping.get(&message).unwrap();
        res.entry(*destination).or_default().insert(message);
    }

    res
}

pub fn compute_message_anonymity_sets(
    trace: &Trace,
    min_delay: Duration,
    max_delay: Duration,
    source_mapping: &HashMap<MessageId, SourceId>,
    destination_mapping: &HashMap<MessageId, DestinationId>,
) -> HashMap<SourceId, Vec<(MessageId, HashMap<DestinationId, (usize, usize)>)>> {
    // for each source, compute its queue of events (messages entering and leaving the network)
    let events = compute_event_queues(trace, min_delay, max_delay);

    // process the event queue, keeping track of which messages
    // are in the window (and thus, the anonymity set)

    // we collect the messages and their per-destination anonymity set descriptors
    // in the following variable.
    // Each (usize,usize) tuple describes:
    // 1. the number of _new_ messages in this anonymity set compared to the
    //    previous one in this source-destination pair.
    // 2. the number of _overlapping_ messages between this anonymity set and the
    //    previous one in this source-destination pair.
    let mut message_anon_sets: Vec<(MessageId, HashMap<DestinationId, (usize, usize)>)> =
        Vec::new();

    // the anonymity set of each source message (that is still in the window)
    let mut current_message_anon_sets: HashMap<MessageId, HashSet<MessageId>> = HashMap::default();

    // also remember the last one per source, so we can compute the relative difference
    // (the previous one is already split by destination)
    let mut last_message_anon_set: HashMap<SourceId, HashMap<DestinationId, HashSet<MessageId>>> =
        HashMap::default();

    // // keep track of the source messages in the current window
    // let mut current_source_messages: HashSet<MessageId> = HashSet::default();

    for event in events {
        match event.event_type {
            EventTypeAndId::AddSourceMessage(_) => {
                // A source message was first observed
                current_message_anon_sets.insert(event.m_id, HashSet::default());
            }
            EventTypeAndId::RemoveSourceMessage(_) => {
                // The "window" of a source message has expired. Consequently,
                // it can be differentiated from messages arriving later than
                // this at their destination.
                let this_message_anon_set = current_message_anon_sets.remove(&event.m_id).unwrap();

                // We now compute this anonymity set's delta from the last anonymity set
                // (per destination) and aggregate it to the numbers of added and shared messages.

                // split the anonymity set by destination
                let this_message_anon_set: std::collections::HashMap<
                    DestinationId,
                    std::collections::HashSet<
                        MessageId,
                        std::hash::BuildHasherDefault<fxhash::FxHasher>,
                    >,
                    std::hash::BuildHasherDefault<fxhash::FxHasher>,
                > = split_by_destination(this_message_anon_set, destination_mapping);

                let source = source_mapping.get(&event.m_id).unwrap();

                // compute the relative difference (per destination) of the new anonymity set,
                // from the anonymity set of the last message of that source
                let relative_difference: HashMap<DestinationId, (usize, usize)> =
                    match last_message_anon_set.get(source) {
                        None => {
                            // all messages are new
                            this_message_anon_set
                                .iter()
                                .map(|(dest, messages)| (dest.clone(), (messages.len(), 0)))
                                .collect()
                        }
                        Some(previous) => {
                            // compute the difference per destination.
                            // Destinations that aren't present anymore are left out (would be (0,0) anyway).
                            this_message_anon_set
                                .iter()
                                .map(|(dest, messages)| {
                                    (
                                        dest.clone(),
                                        match previous.get(&dest) {
                                            None => (messages.len(), 0),
                                            Some(previous_messages) => {
                                                relative_set_distance(previous_messages, &messages)
                                            }
                                        },
                                    )
                                })
                                .collect()
                        }
                    };

                // save the aggregated anonymity set delta as the next result
                message_anon_sets.push((event.m_id, relative_difference));

                // remember the original (but split by destination) anonymity set for next iteration
                last_message_anon_set.insert(source.clone(), this_message_anon_set);
            }
            EventTypeAndId::AddDestinationMessage(_) => {
                // A message arrived at its destination. It is therefore
                // part of the anonymity set of each source message that
                // we haven't removed from the source set yet.
                for anonymity_set in current_message_anon_sets.values_mut() {
                    anonymity_set.insert(event.m_id);
                }
            }
        };
    }

    // Split messages by source
    let mut result: HashMap<SourceId, Vec<(MessageId, HashMap<DestinationId, (usize, usize)>)>> =
        HashMap::default();
    for (message, anon_set) in message_anon_sets {
        let source = source_mapping.get(&message).unwrap();
        result.entry(*source).or_default().push((message, anon_set));
    }

    result
}

fn compute_source_and_destination_mapping(
    trace: &Trace,
) -> (
    HashMap<MessageId, SourceId>,
    HashMap<MessageId, DestinationId>,
) {
    let mut source_mapping = HashMap::default();
    let mut destination_mapping = HashMap::default();
    for entry in trace.entries.iter() {
        source_mapping.insert(entry.m_id, entry.source_id.clone());
        destination_mapping.insert(entry.m_id, entry.destination_id.clone());
    }
    (source_mapping, destination_mapping)
}

fn compute_source_and_destination_message_mapping(
    trace: &Trace,
) -> (
    HashMap<SourceId, Vec<MessageId>>,
    HashMap<DestinationId, Vec<MessageId>>,
) {
    let mut source_message_mapping = HashMap::default();
    let mut destination_message_mapping = HashMap::default();
    for trace_entry in trace.entries.iter() {
        match source_message_mapping.entry(trace_entry.source_id.clone()) {
            Entry::Vacant(e) => {
                e.insert(vec![trace_entry.m_id]);
            }
            Entry::Occupied(mut e) => e.get_mut().push(trace_entry.m_id),
        }
        match destination_message_mapping.entry(trace_entry.destination_id.clone()) {
            Entry::Vacant(e) => {
                e.insert(vec![trace_entry.m_id]);
            }
            Entry::Occupied(mut e) => e.get_mut().push(trace_entry.m_id),
        }
    }
    (source_message_mapping, destination_message_mapping)
}

/* Currently computes this completely from source perspective:
  for each message sent we consider all destinations that received a message in the timeframe (mindelay - maxdelay)
  we should also compute this from the destinations point of view and then intersect those sets.
*/

pub fn compute_relationship_anonymity(
    trace: &Trace,
    min_delay: Duration,
    max_delay: Duration,
) -> Result<
    (
        HashMap<SourceId, Vec<(MessageId, Vec<DestinationId>)>>,
        HashMap<SourceId, Vec<(MessageId, Vec<DestinationId>)>>,
    ),
    Box<dyn std::error::Error>,
> {
    let mut bench = bench::Bench::new();
    let BENCH_ENABLED = true;

    bench.measure("preliminaries for metric calculation", BENCH_ENABLED);
    let (source_message_mapping, destination_message_mapping) =
        compute_source_and_destination_message_mapping(&trace);
    let (source_mapping, destination_mapping) = compute_source_and_destination_mapping(&trace);

    bench.measure("source anonymity sets", BENCH_ENABLED);
    let source_message_anonymity_sets = compute_message_anonymity_sets(
        &trace,
        min_delay,
        max_delay,
        &source_mapping,
        &destination_mapping,
    );

    bench.measure("source relationship anonymity sets", BENCH_ENABLED);
    let source_relationship_anonymity_sets: HashMap<
        SourceId,
        Vec<(MessageId, Vec<DestinationId>)>,
    > = compute_relation_ship_anonymity_sets(source_message_anonymity_sets);
    /* Be wary that this yields only useful results if there is just one source per destination */
    let destination_relationship_anonymity_sets = HashMap::default();
    /*compute_relation_ship_anonymity_sets(
        destination_message_mapping,
        source_mapping,
        destination_message_anonymity_sets,
    )?;*/
    Ok((
        source_relationship_anonymity_sets,
        destination_relationship_anonymity_sets,
    ))
}

pub fn compute_relation_ship_anonymity_sets(
    message_anonymity_sets: HashMap<
        SourceId,
        Vec<(MessageId, HashMap<DestinationId, (usize, usize)>)>,
    >,
) -> HashMap<SourceId, Vec<(MessageId, Vec<DestinationId>)>> {
    //TODO rayon
    // Bitvektoren für Anonymity sets
    let relationship_anonymity_sets = message_anonymity_sets
        .into_par_iter()
        .map(|(source, messages)| {
            // size of the destination anonymity set after each message
            let mut anon_set_sizes = Vec::new();

            // number of candidate messages per destination after the previous message
            let mut prev_destination_candidates: HashMap<DestinationId, usize> = {
                // For the very first  message of this source), pretent all its destinations
                // were seen before (so we do not exclude them now), but there was no
                // candidate messages left. This way, we will just use the first candidate
                // set as-is.
                if let Some((_first_message, first_destinations)) = messages.first() {
                    first_destinations
                        .keys()
                        .cloned()
                        .map(|dest| (dest, 0))
                        .collect()
                } else {
                    HashMap::default()
                }
            };

            // go through all messages and check their potential destinations
            for (source_message, anonymity_set_sizes) in messages {
                // number of candidate messages per destination for this source message
                let mut destination_candidates: HashMap<DestinationId, usize> = HashMap::default();

                for (destination, (added, overlap)) in anonymity_set_sizes {
                    // calculate the number of candidate messages for this destination
                    let from_previous_message = match prev_destination_candidates.get(&destination)
                    {
                        None => {
                            // this destination wasn't a candidate previously, so we don't add it
                            continue;
                        }
                        Some(previous_candidates) => previous_candidates,
                    };

                    let candidates = added + min(*from_previous_message, overlap);

                    // For this destination to remain a candidate, it must have at least one message
                    if candidates == 0 {
                        // Do not keep/make this destination a candidate. This means that our source
                        // was sending more messages than the destination potentially received
                        // from this source.
                        continue;
                    }

                    // This destination is (still) a candidate for our source after this message.
                    // For the next source_message, reduce our candidate message count by one
                    // because we have "used" or "assigned" one of the messages
                    destination_candidates.insert(destination.clone(), candidates - 1);
                }

                // The destination anonymity set after this message is now ready.
                // For now, we only output its size.
                anon_set_sizes.push((
                    source_message,
                    destination_candidates.keys().cloned().collect(),
                ));

                // remember the remaining number of message candidates for each destination
                prev_destination_candidates = destination_candidates;
            }

            (source, anon_set_sizes)
        })
        .collect();

    relationship_anonymity_sets
}

/*
fn compute_relationship_anonymity_intersection(
    source_relationship_anonymity_sets: HashMap<String, Vec<(u64, Vec<String>)>>,
    destination_relationship_anonymity_sets: HashMap<String, Vec<(u64, Vec<String>)>>,
) -> HashMap<String, Vec<(u64, Vec<String>)>> {
    let mut relationship_anonymity_sets: HashMap<u64, Vec<String>> = HashMap::new();
    let mut source_message_map: HashMap<String, Vec<u64>> = HashMap::new();
    let mut source_message_anonymity_sets: HashMap<u64, Vec<String>> = HashMap::new();
    let mut destination_message_map: HashMap<String, Vec<u64>> = HashMap::new();
    let mut destination_message_anonymity_sets: HashMap<u64, Vec<String>> = HashMap::new();

    for (source, mas) in source_relationship_anonymity_sets.into_iter() {
        let mut message_list: Vec<u64> = vec![];
        for (id, destinations) in mas.into_iter() {
            message_list.push(id);
            source_message_anonymity_sets.insert(id, destinations);
        }
        source_message_map.insert(source.to_string(), message_list);
    }

    for (destination, mas) in destination_relationship_anonymity_sets.into_iter() {
        let mut message_list: Vec<u64> = vec![];
        for (id, sources) in mas.into_iter() {
            message_list.push(id);
            destination_message_anonymity_sets.insert(id, sources);
        }
        destination_message_map.insert(destination.to_string(), message_list);
    }

    for (id, sources) in destination_message_anonymity_sets.iter() {
        if let Some(destinations) = source_message_anonymity_sets.get(id) {
            for source in sources
        }
    }
    relationship_anonymity_sets
}
*/

/// Compute the event queues from messages entering and leaving the network, per source.
fn compute_event_queues(
    trace: &Trace,
    min_delay: Duration,
    max_delay: Duration,
) -> Vec<ProcessingEvent> {
    // collect all the sources first and create a hash map entry for each
    // we do this separately first in order to have a complete list of sources
    // later when creating the destination message events
    let mut event_queue: Vec<ProcessingEvent> = Vec::new();

    let max_delay = max_delay + time::Duration::nanoseconds(1); // TODO

    for entry in trace.entries.iter() {
        event_queue.push(ProcessingEvent {
            event_type: EventTypeAndId::AddSourceMessage(entry.source_id),
            ts: entry.source_timestamp.add(min_delay),
            m_id: entry.m_id,
        });

        event_queue.push(ProcessingEvent {
            event_type: EventTypeAndId::RemoveSourceMessage(entry.source_id),
            ts: entry.source_timestamp.add(max_delay),
            m_id: entry.m_id,
        });

        event_queue.push(ProcessingEvent {
            event_type: EventTypeAndId::AddDestinationMessage(entry.destination_id),
            ts: entry.destination_timestamp,
            m_id: entry.m_id,
        });
    }

    event_queue.sort();
    event_queue
}

pub fn write_source_anon_set(
    map: &HashMap<SourceId, Vec<(MessageId, HashMap<DestinationId, (usize, usize)>)>>,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let wtr = std::fs::File::create(path)?;
    serde_json::to_writer_pretty(&wtr, map)?;
    Ok(())
}
pub fn write_sras(
    map: &BTreeMap<MessageId, Vec<DestinationId>>,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let wtr = std::fs::File::create(path)?;
    serde_json::to_writer_pretty(&wtr, map)?;
    Ok(())
}

pub fn read_source_anon_set(
    path: &str,
) -> Result<
    HashMap<SourceId, Vec<(MessageId, HashMap<DestinationId, (usize, usize)>)>>,
    Box<dyn std::error::Error>,
> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    // Read the JSON contents of the file as an instance of `User`.
    let message_anon_set = serde_json::from_reader(reader)?;
    Ok(message_anon_set)
}

pub fn read_sras(
    path: &str,
) -> Result<HashMap<MessageId, Vec<DestinationId>>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    // Read the JSON contents of the file as an instance of `User`.
    let sras = serde_json::from_reader(reader)?;
    Ok(sras)
}

/* TODO to improve debugging, we might want to return WHERE exactly they differ */
fn compare_source_anonymity_sets(
    sas1: HashMap<SourceId, Vec<(MessageId, &HashMap<DestinationId, (usize, usize)>)>>,
    sas2: &HashMap<SourceId, Vec<(MessageId, HashMap<DestinationId, (usize, usize)>)>>,
) -> Result<(), Box<dyn std::error::Error>> {
    for (source_id, messages1) in sas1.iter() {
        let mut messages2 = sas2
            .get(source_id)
            .ok_or(Err::<(), &str>("{source_id} not in sas2"));

        let mut messages1_iter = messages1.iter();
        let mut messages2_iter = messages2.iter();

        while let (Some(m1), Some(m2)) = (messages1_iter.next(), messages2_iter.next()) {
            /* TODO  */
        }
    }
    Ok(())
}
#[derive(Serialize, Deserialize)]
pub struct TestParameters {
    min_delay: i64,
    max_delay: i64,
}
pub fn read_parameters(path: &str) -> Result<TestParameters, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    // Read the JSON contents of the file as an instance of `User`.
    let parameters = serde_json::from_reader(reader)?;
    Ok(parameters)
}

fn append_to_path(p: PathBuf, s: &str) -> PathBuf {
    let mut p = p.into_os_string();
    p.push(s);
    p.into()
}
pub fn simple_example_generator(
    min_delay: i64,
    max_delay: i64,
    network_trace: Trace,
    source_relationship_anonymity_set: HashMap<SourceId, Vec<(MessageId, Vec<DestinationId>)>>,
    path: PathBuf,
) {
    let min_delay = Duration::milliseconds(min_delay);
    let max_delay = Duration::milliseconds(max_delay);
    let net_trace_path = append_to_path(path.clone(), "./net_trace.csv");
    let source_anon_set_path = append_to_path(path.clone(), "./source_anonymity_set.json");
    let sras_path = append_to_path(path, "./sras.json");
    network_trace.write_to_file(&net_trace_path);

    let mut sras_map = BTreeMap::default();
    for (_s_id, sas) in source_relationship_anonymity_set {
        for (m_id, d_ids) in sas {
            sras_map.insert(m_id, d_ids);
        }
    }
    write_sras(&sras_map, &sras_path);
}
#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        path::{Path, PathBuf},
    };

    use crate::metric::*;

    fn execute_test(min_delay: i64, max_delay: i64, path: &str) {
        let parameters = read_parameters(path);
        let min_delay = Duration::milliseconds(min_delay);
        let max_delay = Duration::milliseconds(max_delay);
        let trace_path = String::from(path) + "/network_trace.csv";
        let sras_path = String::from(path) + "/sras.json";
        let network_trace = Trace::from_csv(trace_path).unwrap();
        let expected_sras = read_sras(&sras_path).unwrap();
        let (sras, _) =
            compute_relationship_anonymity(&network_trace, min_delay, max_delay).unwrap();
        let mut n_sras = HashMap::default();
        for (_s_id, sas) in sras {
            for (m_id, d_ids) in sas {
                n_sras.insert(m_id, d_ids);
            }
        }
        assert!(n_sras == expected_sras);
    }
    #[test]
    fn simple_test_1() {
        execute_test(1, 100, "./test/simple_test_1/");
    }
}
