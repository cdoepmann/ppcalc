use rand_distr::Distribution;
use statrs::distribution::Normal;
use ppcalc_metric::SourceId;

use crate::cli::GenerateArgs;
use crate::{bench, destination, network, source, trace};

pub fn run(args: GenerateArgs) -> anyhow::Result<()> {
    let mut bench = bench::Bench::new();
    let BENCH_ENABLED = true;

    let message_num_distr = args
        .num_messages
        .make_distr()
        .map_err(|e| anyhow::anyhow!(e))?;
    let mut rng = rand::thread_rng();

    // traces = trace::read_source_trace_from_file(&source_path).unwrap();

    let source_traces = if let Some(source_path) = args.reuse_sources {
        println!("Reusing sources from {}...", source_path.display());
        bench.measure("read sources", BENCH_ENABLED);
        trace::read_sources_from_trace(&source_path).map_err(|e| anyhow::anyhow!(e))?
    } else {
        println!("Generating new sources...");
        bench.measure("generate sources", BENCH_ENABLED);

        let mut source_traces = vec![];
        for i in 0..args.num_sources {
            let source_id = SourceId::new(i);
            let mut source = source::Source::new(
                message_num_distr.sample(&mut rng),
                args.source_imd.clone(),
                args.source_wait.clone(),
            );
            source_traces.push(source.gen_source_trace(source_id));
        }
        // write_sources(&source_path, &source_traces).unwrap();
        source_traces
    };

    bench.measure("generating source-destination map ", BENCH_ENABLED);
    let source_name_list = source_traces.iter().map(|x| x.source_id.clone()).collect();
    let source_destination_map = destination::destination_selection(
        &args.destination_selection_type,
        args.num_destinations,
        source_name_list,
    );

    bench.measure("merge traces", BENCH_ENABLED);
    let pre_network_trace = network::merge_traces(source_traces, &source_destination_map);
    let network_trace = network::generate_network_delay(&args.network_delay, pre_network_trace);

    bench.measure("write to file", BENCH_ENABLED);
    network_trace
        .write_to_file(&args.output)
        .map_err(|e| anyhow::anyhow!(e))?;

    // TODO
    // bench.measure("parameters", BENCH_ENABLED);
    // let parameter_path = String::from(&working_dir) + "parameters.json";
    // std::fs::write(parameter_path, serde_json::to_string_pretty(&args).unwrap()).unwrap();

    Ok(())
}
