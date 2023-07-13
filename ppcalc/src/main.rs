#[macro_use]
extern crate derive_builder;

mod bench;
mod cli;
mod destination;
mod network;
mod plot;
mod source;
mod trace;
use rand_distr::Distribution;
use statrs::distribution::Normal;
use std::{env, fs, path::Path};
use time::Duration;
use trace::write_sources;

use ppcalc_metric::SourceId;

use crate::cli::ParametersBuilderError;

fn main() {
    let mut bench = bench::Bench::new();
    let bench_enabled = true;

    bench.measure("Command line parsing", bench_enabled);
    let params = cli::cli_parsing(env::args());
    let params = match params {
        Ok(param) => param,
        Err(err) => match err {
            ParametersBuilderError::UninitializedField(_) => {
                panic!("{err}\n You may want to use default values via --default");
            }
            ParametersBuilderError::ValidationError(_) => {
                panic!("Could not parse cli : {err}");
            }
        },
    };

    let working_dir = String::from("./sim/") + params.experiment.as_str() + "/";
    let message_distr = Normal::new(params.num_messages_mean, params.num_messages_dev).unwrap();
    let mut rng = rand::thread_rng();
    let mut traces = vec![];
    fs::create_dir_all(working_dir.clone()).unwrap();
    let source_dir = working_dir.clone() + "../../../ppcalc-data/" + params.experiment.as_str();
    let source_path = source_dir.clone() + "/sources.json";
    let source_file_exists: bool = Path::new(&source_path).exists();

    if source_file_exists || !params.reuse_sources {
        println!("Note! You did not reuse the sources, even though they are available")
    }

    if params.reuse_sources {
        bench.measure("reading sources", bench_enabled);
        traces = trace::read_source_trace_from_file(&source_path).unwrap();
        println!("Reusing sources");
    } else {
        bench.measure("generate sources", bench_enabled);
        for i in 0..params.num_sources {
            let source_id = SourceId::new(i);
            let mut source = source::Source::new(
                message_distr.sample(&mut rng).ceil() as u64,
                Normal::new(params.source_imd_mean, params.source_imd_dev).unwrap(),
                Normal::new(params.source_wait_mean, params.source_wait_dev).unwrap(),
            );
            traces.push(source.gen_source_trace(source_id));
        }
        fs::create_dir_all(&source_dir.clone()).unwrap();
        write_sources(&source_path, &traces).unwrap();
    }

    // Not needed but to ensure CSV stuff is working
    let mut job_id = String::from("JOB_ID");
    match env::var(job_id.clone()) {
        Ok(v) => job_id = v,
        Err(e) => job_id = String::from("Buergergeld_is_real"),
    }

    bench.measure("generating destinations", bench_enabled);

    let working_dir = working_dir.clone()
        + "./"
        + params.num_destinations.to_string().as_str()
        + "/"
        + job_id.as_str()
        + "/";
    fs::create_dir_all(working_dir.clone()).unwrap();
    let source_destination_map_path = working_dir.to_string() + "/source_destination_map";
    bench.measure("generating source-destination map ", bench_enabled);
    let source_name_list = traces.iter().map(|x| x.source_id.clone()).collect();
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
    bench.measure("merge traces", bench_enabled);
    let pre_network_trace = network::merge_traces(traces, &source_destination_map);
    let network_trace = network::generate_network_delay(
        params.network_delay_min,
        params.network_delay_max,
        pre_network_trace,
    );
    /*network_trace
        .write_to_file("./sim/network_trace.csv")
        .unwrap();
    let network_trace = ppcalc_metric::Trace::from_csv("./sim/network_trace.csv").unwrap();
    */
    bench.measure("anonymity metric calculation", bench_enabled);
    let (source_relationship_anonymity_sets, _destination_relationship_anonymity_sets) =
        ppcalc_metric::compute_relationship_anonymity(
            &network_trace,
            Duration::milliseconds(params.network_delay_min),
            Duration::milliseconds(params.network_delay_max),
        )
        .unwrap();

    // bench.measure("plot", BENCH_ENABLED);
    // let plot = plot::PlotFormat::new(source_relationship_anonymity_sets, source_destination_map);
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
    bench.measure("deanomization", bench_enabled);
    let deanomization_path = String::from(&working_dir) + "/deanomization.json";
    // let deanomization_vec = plot.deanonymized_users_over_time();
    // std::fs::write(
    //     deanomization_path,
    //     serde_json::to_string_pretty(&deanomization_vec).unwrap(),
    // )
    // .unwrap();

    bench.measure("parameters", bench_enabled);
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

    if let Some(path) = params.testcase_path {
        ppcalc_metric::simple_example_generator(
            params.network_delay_min,
            params.network_delay_max,
            network_trace,
            source_relationship_anonymity_sets,
            path.into(),
        )
    }
}
