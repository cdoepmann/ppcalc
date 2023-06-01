mod analytics;
mod destination;
mod network;
mod plot;
mod source;
mod trace;
use core::num;
use csv::WriterBuilder;
use rand_distr::Distribution;
use serde::{Deserialize, Serialize};
use statrs::distribution::Normal;
use std::{env, fs, path::Path};
use trace::write_sources;
fn help() {
    println!("Help is currently not available. Please panic");
}

#[derive(Serialize, Deserialize)]
struct Parameters {
    reuse_sources: bool,
    experiment: String,
    destination_selection_type: destination::DestinationSelectionType,
    num_sources: u64,
    num_destinations: u64,
    source_imd_mean: f64,
    source_imd_dev: f64,
    source_wait_mean: f64,
    source_wait_dev: f64,
    num_messages_mean: f64,
    num_messages_dev: f64,
    network_delay_min: i64,
    network_delay_max: i64,
}

impl Parameters {
    fn default() -> Parameters {
        Parameters {
            reuse_sources: false,
            destination_selection_type: destination::DestinationSelectionType::Uniform,
            num_sources: 1337,
            num_destinations: 1337,
            source_imd_mean: 100.0,
            source_imd_dev: 10.0,
            source_wait_mean: 50000.0,
            source_wait_dev: 1000.0,
            num_messages_mean: 100.0,
            num_messages_dev: 10.0,
            network_delay_min: 1,
            network_delay_max: 100,
            experiment: String::from("experiment1"),
        }
    }
}
fn main() {
    let mut args = env::args().skip(1);
    let mut params = Parameters::default();

    while let Some(arg) = args.next() {
        match &arg[..] {
            "-h" | "--help" => help(),
            "-r" => params.reuse_sources = true,
            "-s" | "--sources" => {
                if let Some(arg_config) = args.next() {
                    params.num_sources = arg_config.parse().unwrap();
                } else {
                    panic!("No value specified for parameter -s");
                }
            }
            "-d" | "--destinations" => {
                if let Some(arg_config) = args.next() {
                    params.num_destinations = arg_config.parse().unwrap();
                } else {
                    panic!("No value specified for parameter -d");
                }
            }
            "--destination_selection" => {
                if let Some(arg_config) = args.next() {
                    match arg_config.as_str() {
                        "normal" => {
                            params.destination_selection_type =
                                destination::DestinationSelectionType::Normal
                        }
                        "uniform" => {
                            params.destination_selection_type =
                                destination::DestinationSelectionType::Uniform
                        }
                        "roundrobin" => {
                            params.destination_selection_type =
                                destination::DestinationSelectionType::RoundRobin
                        }
                        "smallworld" => {
                            params.destination_selection_type =
                                destination::DestinationSelectionType::SmallWorld
                        }
                        _ => {
                            panic!("Wrong argument for --sender-im-distr");
                        }
                    }
                } else {
                    panic!("No value specified for parameter --destination-selection");
                }
            }
            "-e" | "--experiment" => {
                if let Some(arg_config) = args.next() {
                    params.experiment = arg_config.parse().unwrap();
                } else {
                    panic!("No value specified for parameter -e");
                }
            }
            "--source_imd_distr" => {
                if let Some(arg_config) = args.next() {
                    match arg_config
                        .split(":")
                        .into_iter()
                        .collect::<Vec<&str>>()
                        .as_slice()
                    {
                        [arg_source_mean, arg_source_dev] => {
                            params.source_imd_mean = arg_source_mean.parse().unwrap();
                            params.source_imd_dev = arg_source_dev.parse().unwrap();
                        }
                        _ => {
                            panic!("Wrong argument for --sender-im-distr");
                        }
                    }
                } else {
                    panic!("No value specified for parameter --config.");
                }
            }
            "--source_wait_distr" => {
                if let Some(arg_config) = args.next() {
                    match arg_config
                        .split(":")
                        .into_iter()
                        .collect::<Vec<&str>>()
                        .as_slice()
                    {
                        [arg_source_wait_mean, arg_source_wait_dev] => {
                            params.source_wait_mean = arg_source_wait_mean.parse().unwrap();
                            params.source_wait_dev = arg_source_wait_dev.parse().unwrap();
                        }
                        _ => {
                            panic!("Wrong argument for --sender-im-distr");
                        }
                    }
                } else {
                    panic!("No value specified for parameter --config.");
                }
            }
            "--num_messages_distr" => {
                if let Some(arg_config) = args.next() {
                    match arg_config
                        .split(":")
                        .into_iter()
                        .collect::<Vec<&str>>()
                        .as_slice()
                    {
                        [arg_num_messages_mean, arg_num_messages_dev] => {
                            params.num_messages_mean = arg_num_messages_mean.parse().unwrap();
                            params.num_messages_dev = arg_num_messages_dev.parse().unwrap();
                        }
                        _ => {
                            panic!("Wrong argument for --sender-im-distr");
                        }
                    }
                } else {
                    panic!("No value specified for parameter --config.");
                }
            }
            "--network_delay" => {
                if let Some(arg_config) = args.next() {
                    match arg_config
                        .split(":")
                        .into_iter()
                        .collect::<Vec<&str>>()
                        .as_slice()
                    {
                        [arg_network_delay_min, arg_network_delay_max] => {
                            params.network_delay_min = arg_network_delay_min.parse().unwrap();
                            params.network_delay_max = arg_network_delay_max.parse().unwrap();
                        }
                        _ => {
                            panic!("Wrong argument for --sender-im-distr");
                        }
                    }
                } else {
                    panic!("No value specified for parameter --config.");
                }
            }
            _ => {
                if arg.starts_with('-') {
                    println!("Unkown argument {}", arg);
                } else {
                    println!("Unkown positional argument {}", arg);
                }
            }
        }
    }

    let mut traces: Vec<trace::SourceTrace> = vec![];
    let working_dir = String::from("./sim/") + params.experiment.as_str() + "/";
    let message_distr = Normal::new(params.num_messages_mean, params.num_messages_dev).unwrap();
    let mut rng = rand::thread_rng();
    let mut traces = vec![];
    fs::create_dir_all(working_dir.clone()).unwrap();
    let source_path =
        working_dir.clone() + "../../../ppcalc-data/" + params.experiment.as_str() + "/sources.json";
    write_sources(&source_path, &traces).unwrap();

    let mut source_file_exists: bool = true;
    source_file_exists = Path::new(&source_path).exists();

    if params.reuse_sources || source_file_exists {
        traces = trace::read_source_trace_from_file(&source_path).unwrap();
        println!("Reusing sources");
    } else {
        for i in 0..params.num_sources {
            let mut source = source::Source::new(
                message_distr.sample(&mut rng).ceil() as u64,
                Normal::new(params.source_imd_mean, params.source_imd_dev).unwrap(),
                Normal::new(params.source_wait_mean, params.source_wait_dev).unwrap(),
            );
            traces.push(source.gen_source_trace(String::from("s") + &i.to_string()));
        }
    }

    // Not needed but to ensure CSV stuff is working
    let mut job_id = String::from("JOB_ID");
    match env::var(job_id.clone()) {
        Ok(v) => job_id = v,
        Err(e) => panic!("${} is not set ({})", job_id, e),
    }

    let env_num_destinations = String::from("NUM_DESTINATIONS");
    match env::var(env_num_destinations.clone()) {
        Ok(v) => params.num_destinations = v.parse().unwrap(),
        Err(_e) => println!("NUM_DESTINATIONS environment variable has not been set"),
    }
    let working_dir = working_dir.clone()
        + "./"
        + params.num_destinations.to_string().as_str()
        + "/"
        + job_id.as_str()
        + "/";
    fs::create_dir_all(working_dir.clone()).unwrap();
    let source_destination_map_path = working_dir.to_string() + "/source_destination_map";

    let source_name_list = traces.iter().map(|x| x.source_name.clone()).collect();
    let source_destination_map = destination::destination_selection(
        &params.destination_selection_type,
        params.num_destinations,
        source_name_list,
    );
    trace::write_source_destination_map(&source_destination_map, &source_destination_map_path)
        .unwrap();

    // Not needed but to ensure CSV stuff is working
    /*let source_destination_map =
        trace::read_source_destination_map_from_file(source_destination_map_path).unwrap();
    */
    let pre_network_trace = network::merge_traces(traces, &source_destination_map);
    let network_trace = network::generate_network_delay(
        params.network_delay_min,
        params.network_delay_max,
        pre_network_trace,
    );
    /*network_trace
        .write_to_file("./sim/network_trace.csv")
        .unwrap();
    let network_trace = trace::read_network_trace_from_file("./sim/network_trace.csv").unwrap();
    */
    let (source_anonymity_sets, destination_anonymity_sets) =
        analytics::compute_message_anonymity_sets(
            &network_trace,
            params.network_delay_min,
            params.network_delay_max,
        )
        .unwrap();
    let (source_relationship_anonymity_sets, destination_relationship_anonymity_sets) =
        analytics::compute_relationship_anonymity(
            &network_trace,
            params.network_delay_min,
            params.network_delay_max,
        )
        .unwrap();

    let plot = plot::PlotFormat::new(source_relationship_anonymity_sets, source_destination_map);
    /*
    /* for (source, iterative_anonymity_sets) in source_relationship_anonymity_sets.iter() {
            println!("{}", source);
            for (m_id, potential_destinations) in iterative_anonymity_sets {
                println!("{} -> {:?}", m_id, potential_destinations);
            }
        }
    */
    let plot_path = String::from("playbook/plot.json");
    plot.write_plot(plot_path);

    */
    /*let map = plot.anonymity_set_size_over_time();
        std::fs::write(
            "playbook/map.json",
            serde_json::to_string_pretty(&map).unwrap(),
        )
        .unwrap();
    */
    let deanomization_path = String::from(&working_dir) + "/deanomization.json";
    let deanomization_vec = plot.deanonymized_users_over_time();
    std::fs::write(
        deanomization_path,
        serde_json::to_string_pretty(&deanomization_vec).unwrap(),
    )
    .unwrap();

    let parameter_path = String::from(&working_dir) + "parameters.json";
    std::fs::write(
        parameter_path,
        serde_json::to_string_pretty(&params).unwrap(),
    )
    .unwrap();
    /*
    for (destination, iterative_anonymity_sets) in destination_relationship_anonymity_sets.iter() {
        println!("{}", destination);
        for (m_id, potential_source) in iterative_anonymity_sets {
            println!("{} -> {:?}", m_id, potential_source);
        }
    }
    */
}
