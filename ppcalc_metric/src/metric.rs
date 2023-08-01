use std::cmp::min;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::{fs::File, io::BufReader};

use fxhash::FxHashMap as HashMap;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use time::Duration;

use crate::bench;
use crate::containers::MessageSet;
use crate::trace::{
    DestinationId, DestinationMapping, MessageId, SourceId, Trace, TraceBuilder, TraceEntry,
};

/// Compute the relative difference between two message anonymity sets.
///
/// Returns a pair of `usize`s, with the following meaning:
/// 1. the number of _newly added_ messages from `set1` to `set2`
/// 2. the number of _shared_ messages (overlap) between the two sets
fn relative_set_distance(set1: &MessageSet, set2: &MessageSet) -> (usize, usize) {
    set1.distance(set2)
}

/// Split an anonymity set by the destination of its messages
fn split_by_destination(
    set: MessageSet,
    destination_mapping: &DestinationMapping,
) -> HashMap<DestinationId, MessageSet> {
    set.split_by(|message| *destination_mapping.get(&message).unwrap())
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
    Box<dyn std::error::Error + Send + Sync>,
> {
    compute_relationship_anonymity_inner::<OutputFull>(trace, min_delay, max_delay)
}

pub fn compute_relationship_anonymity_sizes(
    trace: &Trace,
    min_delay: Duration,
    max_delay: Duration,
) -> Result<
    (
        HashMap<SourceId, Vec<(MessageId, usize)>>,
        HashMap<SourceId, Vec<(MessageId, usize)>>,
    ),
    Box<dyn std::error::Error + Send + Sync>,
> {
    compute_relationship_anonymity_inner::<OutputSizes>(trace, min_delay, max_delay)
}

fn compute_relationship_anonymity_inner<T: OutputMapper>(
    trace: &Trace,
    min_delay: Duration,
    max_delay: Duration,
) -> Result<
    (
        HashMap<SourceId, Vec<(MessageId, T::Item)>>,
        HashMap<SourceId, Vec<(MessageId, T::Item)>>,
    ),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let mut bench = bench::Bench::new();
    let BENCH_ENABLED = true;

    bench.measure("compute anonymity sets", BENCH_ENABLED);
    let source_relationship_anonymity_sets =
        compute_message_anonymity_sets::<T>(&trace, min_delay, max_delay);

    /* Be wary that this yields only useful results if there is just one source per destination */
    let destination_relationship_anonymity_sets = HashMap::default(); // TODO
    Ok((
        source_relationship_anonymity_sets,
        destination_relationship_anonymity_sets,
    ))
}

/// Helper object to merge the "condensed" anonymity sets of a source into a
/// sequence of destination candidates (or the number thereof).
struct AnonymitySetMerger {
    // number of candidate messages per destination after the previous message
    prev_destination_candidates: Option<HashMap<DestinationId, usize>>,
}

impl AnonymitySetMerger {
    fn new() -> AnonymitySetMerger {
        AnonymitySetMerger {
            prev_destination_candidates: None,
        }
    }

    fn next_anonymity_set(
        &mut self,
        source_message: MessageId,
        destination_anon_sets: &HashMap<DestinationId, (usize, usize)>,
    ) -> Vec<DestinationId> {
        // Access the previous message's candidates
        let prev_destination_candidates = match self.prev_destination_candidates {
            Some(ref mut x) => x,
            None => {
                // For the very first  message of this source), pretent all its destinations
                // were seen before (so we do not exclude them now), but there was no
                // candidate messages left. This way, we will just use the first candidate
                // set as-is.
                self.prev_destination_candidates.insert(
                    destination_anon_sets
                        .keys()
                        .cloned()
                        .map(|dest| (dest, 0))
                        .collect(),
                )
            }
        };

        // number of candidate messages per destination for this source message
        let mut destination_candidates: HashMap<DestinationId, usize> = HashMap::default();

        for (destination, (added, overlap)) in destination_anon_sets {
            // calculate the number of candidate messages for this destination
            let from_previous_message = match prev_destination_candidates.get(&destination) {
                None => {
                    // this destination wasn't a candidate previously, so we don't add it
                    continue;
                }
                Some(previous_candidates) => previous_candidates,
            };

            let candidates = added + min(*from_previous_message, *overlap);

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
        let result = destination_candidates.keys().cloned().collect();

        // remember the remaining number of message candidates for each destination
        *prev_destination_candidates = destination_candidates;

        result
    }
}

/// A filter to replace the returned anonymity set by something else
trait OutputMapper {
    type Item: Send;

    fn map(anonymity_set: Vec<DestinationId>) -> Self::Item;
}

/// Output the full anonymity sets
struct OutputFull;

impl OutputMapper for OutputFull {
    type Item = Vec<DestinationId>;

    fn map(anonymity_set: Vec<DestinationId>) -> Self::Item {
        // change nothing
        anonymity_set
    }
}

/// Output only the anonymity set sizes
struct OutputSizes;

impl OutputMapper for OutputSizes {
    type Item = usize;

    fn map(anonymity_set: Vec<DestinationId>) -> Self::Item {
        anonymity_set.len()
    }
}

fn compute_message_anonymity_sets<T: OutputMapper>(
    trace: &Trace,
    min_delay: Duration,
    max_delay: Duration,
) -> HashMap<SourceId, Vec<(MessageId, T::Item)>> {
    let destination_mapping = trace.get_destination_mapping();

    // split messages per source
    let messages_per_source: Vec<Vec<&TraceEntry>> = {
        let mut v = vec![Vec::new(); trace.max_source_id().to_num() as usize + 1];
        for msg in trace.entries() {
            v.get_mut(msg.source_id.to_num() as usize)
                .unwrap()
                .push(msg);
        }
        v
    };

    // Progress printer. Takes progress info via a channel from the processing
    // threads and prints status info to stdout. This thread finishes as soon
    // a false value is sent to the channel, or the channel is closed.
    let (progress_s, progress_r) = crossbeam_channel::unbounded::<bool>();
    let thread_handle = std::thread::spawn(move || {
        println!("Processing sources...");
        let mut seen: usize = 0;
        while let Ok(value) = progress_r.recv() {
            if value == false {
                break;
            }
            seen += 1;
            if seen % 1000 == 0 && seen > 0 {
                println!("Processed {} sources...", seen);
            }
        }
    });

    let result: HashMap<SourceId, Vec<(MessageId, T::Item)>> = messages_per_source
        .into_par_iter()
        .enumerate()
        .map(|(source, messages)| {
            let source = SourceId::new(source as u64);
            let entries = trace.entries_vec();

            let mut source_result = Vec::new();
            let mut last_msg_anonset: Option<HashMap<DestinationId, MessageSet>> = None;

            // helper struct to merge/intersect the anonymity sets over time
            // (this was previously the "second phase")
            let mut anonset_intersector = AnonymitySetMerger::new();

            for message in messages {
                // Find the relevant destination messages.
                // This exploits the fact that the trace entries are sorted by
                // time of arrival at the destination, so we can carry out fast
                // range queries.
                let mut this_msg_anonset = MessageSet::new();
                let from_time = message.source_timestamp + min_delay;
                let to_time = message.source_timestamp + max_delay;

                // Find the first relevant index (whose timestamp is _not_ less
                // than from_time). We use partition_point(...) here instead of
                // binary_search(...), because the latter would give us only
                // _some_ matching entry, not necessarily the first one.
                let start_index = entries.partition_point(|e| e.destination_timestamp < from_time);

                for dest_msg in &entries[start_index..] {
                    if dest_msg.destination_timestamp > to_time {
                        break;
                    }

                    this_msg_anonset.insert(dest_msg.m_id);
                }

                let this_msg_anonset = split_by_destination(this_msg_anonset, destination_mapping);

                // compute the relative difference (per destination) of the new anonymity set,
                // from the anonymity set of the last message of that source
                let relative_difference: HashMap<DestinationId, (usize, usize)> =
                    match last_msg_anonset {
                        None => {
                            // all messages are new
                            this_msg_anonset
                                .iter()
                                .map(|(dest, messages)| (dest.clone(), (messages.len(), 0)))
                                .collect()
                        }
                        Some(previous) => {
                            // compute the difference per destination.
                            // Destinations that aren't present anymore are left out (would be (0,0) anyway).
                            this_msg_anonset
                                .iter()
                                .map(|(dest, messages)| {
                                    (
                                        dest.clone(),
                                        match previous.get(&dest) {
                                            None => (messages.len(), 0),
                                            Some(previous_messages) => {
                                                relative_set_distance(previous_messages, messages)
                                            }
                                        },
                                    )
                                })
                                .collect()
                        }
                    };

                // use the aggregated anonymity set delta for computing the next anonymity set (possible destinations)
                let anonymity_set =
                    anonset_intersector.next_anonymity_set(message.m_id, &relative_difference);

                // map the anonymity set to what we want to output
                let anonymity_set = T::map(anonymity_set);

                // save it as the next result
                source_result.push((message.m_id, anonymity_set));

                // remember the original (but split by destination) anonymity set for next iteration
                last_msg_anonset = Some(this_msg_anonset);
            }
            progress_s.send(true).unwrap();
            (source, source_result)
        })
        .collect();

    progress_s.send(false).unwrap();
    thread_handle.join().unwrap();
    println!("done.");

    result
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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
pub fn read_parameters(path: &Path) -> Result<TestParameters, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    // Read the JSON contents of the file as an instance of `User`.
    let parameters = serde_json::from_reader(reader)?;
    Ok(parameters)
}

pub fn write_parameters(
    params: TestParameters,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let wtr = std::fs::File::create(path)?;
    serde_json::to_writer_pretty(&wtr, &params)?;
    Ok(())
}

fn append_to_path(p: PathBuf, s: &str) -> PathBuf {
    let mut p = p;
    p.push(s);
    p.into()
}
pub fn simple_example_generator(
    min_delay: i64,
    max_delay: i64,
    network_trace: &Trace,
    source_relationship_anonymity_set: HashMap<SourceId, Vec<(MessageId, Vec<DestinationId>)>>,
    path: PathBuf,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    fs::create_dir_all(path.clone())?;
    let net_trace_path = append_to_path(path.clone(), "network_trace.csv");
    let sras_path = append_to_path(path.clone(), "sras.json");
    network_trace.write_to_file(&net_trace_path)?;

    let mut sras_map = BTreeMap::default();
    for (_s_id, sas) in source_relationship_anonymity_set {
        for (m_id, d_ids) in sas {
            sras_map.insert(m_id, d_ids);
        }
    }
    write_sras(&sras_map, &sras_path)?;

    let parameter_path = append_to_path(path.clone(), "params.json");
    let params = TestParameters {
        min_delay: min_delay,
        max_delay: max_delay,
    };
    write_parameters(params, &parameter_path)?;

    Ok(())
}
#[cfg(test)]
mod tests {
    use crate::metric::*;

    fn execute_test(path: &str) {
        let parameter_path = append_to_path(path.clone().into(), "./params.json");
        let parameters = read_parameters(&parameter_path).unwrap();
        let min_delay = Duration::milliseconds(parameters.min_delay);
        let max_delay = Duration::milliseconds(parameters.max_delay);
        let trace_path = String::from(path) + "/network_trace.csv";
        let sras_path = String::from(path) + "/sras.json";
        let network_trace = TraceBuilder::from_csv(trace_path).unwrap().build().unwrap();
        let mut expected_sras = read_sras(&sras_path).unwrap();
        let (sras, _) =
            compute_relationship_anonymity(&network_trace, min_delay, max_delay).unwrap();
        let mut n_sras = HashMap::default();
        for (_s_id, sas) in sras {
            for (m_id, d_ids) in sas {
                n_sras.insert(m_id, d_ids);
            }
        }

        // sort destination candidate vectors to make them comparable
        for map in [&mut n_sras, &mut expected_sras] {
            for dests in map.values_mut() {
                dests.sort();
            }
        }

        assert!(n_sras == expected_sras);
    }
    #[test]
    fn simple_test_1() {
        execute_test("./test/simple_test_1/");
    }
    #[test]
    fn simple_test_2() {
        execute_test("./test/simple_test_2/");
    }

    #[test]
    fn simple_test_3() {
        execute_test("./test/simple_test_3/");
    }
    #[test]
    fn simple_test_4() {
        execute_test("./test/simple_test_4/");
    }

    #[test]
    fn simple_test_5() {
        execute_test("./test/simple_test_5/");
    }

    #[test]
    fn simple_test_6() {
        execute_test("./test/simple_test_6/");
    }

    #[test]
    fn simple_test_7() {
        execute_test("./test/simple_test_7/");
    }
}
