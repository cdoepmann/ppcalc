mod analytics;
mod destination;
mod network;
mod source;
mod trace;
use std::fs;
fn main() {
    let mut traces: Vec<trace::SourceTrace> = vec![];
    let mut source = source::Source::new(13, 5, 50);
    traces.push(source.gen_source_trace(String::from("Alice")));
    traces.push(source.gen_source_trace(String::from("Bob")));

    let trace_dir = "./sim/traces/";
    fs::create_dir_all(trace_dir).unwrap();
    for trace in traces.iter() {
        let path = String::from(trace_dir) + &trace.source_name;
        trace.write_to_file(&path).unwrap();
    }

    // Not needed but to ensure CSV stuff is working
    let mut traces = vec![];
    let paths = fs::read_dir(trace_dir).unwrap();
    for path in paths {
        traces.push(
            trace::read_source_trace_from_file(path.unwrap().path().to_str().unwrap()).unwrap(),
        );
    }
    let source_destination_map_path = "./sim/source_destination_map";
    let source_name_list = traces.iter().map(|x| x.source_name.clone()).collect();
    let source_destination_map = destination::uniform_destination_selection(2, source_name_list);
    trace::write_source_destination_map(source_destination_map, source_destination_map_path)
        .unwrap();

    // Not needed but to ensure CSV stuff is working
    let source_destination_map =
        trace::read_source_destination_map_from_file(source_destination_map_path).unwrap();

    let pre_network_trace = network::merge_traces(traces, source_destination_map);
    let network_trace = network::generate_network_delay(1, 100, pre_network_trace);
    network_trace
        .write_to_file("./sim/network_trace.csv")
        .unwrap();
    let network_trace = trace::read_network_trace_from_file("./sim/network_trace.csv").unwrap();
    let (source_anonymity_sets, destination_anonymity_sets) =
        analytics::compute_message_anonymity_sets(&network_trace, 1, 100).unwrap();
    let (source_relationship_anonymity_sets, destination_relationship_anonymity_sets) =
        analytics::compute_relationship_anonymity(&network_trace, 1, 100).unwrap();

    for (k, v) in source_anonymity_sets.iter() {
        print!("{}: ", k);
        for id in v {
            print!("{} ", id);
        }
        println!("");
    }
    for (k, v) in destination_anonymity_sets.iter() {
        print!("{}: ", k);
        for id in v {
            print!("{} ", id);
        }
        println!("");
    }

    for (source, iterative_anonymity_sets) in source_relationship_anonymity_sets.iter() {
        println!("{}", source);
        for (m_id, potential_destinations) in iterative_anonymity_sets {
            println!("{} -> {:?}", m_id, potential_destinations);
        }
    }
    for (destination, iterative_anonymity_sets) in destination_relationship_anonymity_sets.iter() {
        println!("{}", destination);
        for (m_id, potential_source) in iterative_anonymity_sets {
            println!("{} -> {:?}", m_id, potential_source);
        }
    }
}
