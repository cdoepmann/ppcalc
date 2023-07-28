use std::{env, fs, path::Path};

use rand_distr::Distribution;
use statrs::distribution::Normal;
use ppcalc_metric::SourceId;

use crate::cli::GenerateArgs;
use crate::trace::write_sources;
use crate::{bench, destination, network, source, trace};

pub fn run(args: GenerateArgs) -> anyhow::Result<()> {
    let mut bench = bench::Bench::new();
    let BENCH_ENABLED = true;

    let working_dir = String::from("./sim/") + args.experiment.as_str() + "/";
    let message_distr = Normal::new(args.num_messages_mean, args.num_messages_dev).unwrap();
    let mut rng = rand::thread_rng();
    let mut traces = vec![];
    fs::create_dir_all(working_dir.clone()).unwrap();
    let source_dir = working_dir.clone() + "../../../ppcalc-data/" + args.experiment.as_str();
    let source_path = source_dir.clone() + "/sources.json";
    let source_file_exists: bool = Path::new(&source_path).exists();

    if source_file_exists && !args.reuse_sources {
        println!("Note! You did not reuse the sources, even though they are available")
    }

    if args.reuse_sources {
        bench.measure("reading sources", BENCH_ENABLED);
        traces = trace::read_source_trace_from_file(&source_path).unwrap();
        println!("Reusing sources");
    } else {
        bench.measure("generate sources", BENCH_ENABLED);
        for i in 0..args.num_sources {
            let source_id = SourceId::new(i);
            let mut source = source::Source::new(
                message_distr.sample(&mut rng).ceil() as u64,
                Normal::new(args.source_imd_mean, args.source_imd_dev).unwrap(),
                Normal::new(args.source_wait_mean, args.source_wait_dev).unwrap(),
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
        Err(_) => job_id = String::from("Buergergeld_is_real"),
    }

    bench.measure("generating destinations", BENCH_ENABLED);
    // let env_num_destinations = String::from("NUM_DESTINATIONS");
    // match env::var(env_num_destinations.clone()) {
    //     Ok(v) => args.num_destinations = v.parse().unwrap(),
    //     Err(_e) => println!("NUM_DESTINATIONS environment variable has not been set"),
    // }
    let working_dir = working_dir.clone()
        + "./"
        + args.num_destinations.to_string().as_str()
        + "/"
        + job_id.as_str()
        + "/";
    fs::create_dir_all(working_dir.clone()).unwrap();
    let source_destination_map_path = working_dir.to_string() + "/source_destination_map";
    bench.measure("generating source-destination map ", BENCH_ENABLED);
    let source_name_list = traces.iter().map(|x| x.source_id.clone()).collect();
    let source_destination_map = destination::destination_selection(
        &args.destination_selection_type,
        args.num_destinations,
        source_name_list,
    );
    trace::write_source_destination_map(&source_destination_map, &source_destination_map_path)
        .unwrap();

    // Not needed but to ensure CSV stuff is working
    /*let source_destination_map =
        trace::read_source_destination_map_from_file(source_destination_map_path).unwrap();
    */
    bench.measure("merge traces", BENCH_ENABLED);
    let pre_network_trace = network::merge_traces(traces, &source_destination_map);
    let network_trace = network::generate_network_delay(
        args.network_delay_min,
        args.network_delay_max,
        pre_network_trace,
    );

    bench.measure("write to file", BENCH_ENABLED);
    network_trace.write_to_file(&args.output).unwrap();

    // TODO
    // bench.measure("parameters", BENCH_ENABLED);
    // let parameter_path = String::from(&working_dir) + "parameters.json";
    // std::fs::write(parameter_path, serde_json::to_string_pretty(&args).unwrap()).unwrap();

    Ok(())
}
