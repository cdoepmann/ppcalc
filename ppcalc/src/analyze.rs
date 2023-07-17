use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::anyhow;
use ppcalc_metric::TraceBuilder;
use time::Duration;

use crate::cli::AnalyzeArgs;

use ppcalc_metric::{DestinationId, MessageId, SourceId, Trace};

pub fn run(args: AnalyzeArgs) -> anyhow::Result<()> {
    let network_trace = TraceBuilder::from_csv(&args.input)
        .map_err(|e| anyhow!(e))?
        .build()?;
    let (source_relationship_anonymity_sets, _destination_relationship_anonymity_sets) =
        ppcalc_metric::compute_relationship_anonymity(
            &network_trace,
            Duration::milliseconds(args.min_window as i64),
            Duration::milliseconds(args.max_window as i64),
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
    // bench.measure("deanomization", BENCH_ENABLED);
    // let deanomization_path = String::from(&working_dir) + "/deanomization.json";
    // let deanomization_vec = plot.deanonymized_users_over_time();
    // std::fs::write(
    //     deanomization_path,
    //     serde_json::to_string_pretty(&deanomization_vec).unwrap(),
    // )
    // .unwrap();

    if let Some(path) = args.generate_testcase {
        ppcalc_metric::simple_example_generator(
            args.min_window as i64,
            args.max_window as i64,
            &network_trace,
            source_relationship_anonymity_sets,
            path.into(),
        )
    }

    Ok(())
}
