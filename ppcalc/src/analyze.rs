use std::fs;

use anyhow::anyhow;
use ppcalc_metric::TraceBuilder;
use time::Duration;

use crate::cli::AnalyzeArgs;
use crate::plot::deanonymized_users_over_time;

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
        .map_err(|e| anyhow!(e))?;

    if let Some(path) = args.output_user_anonsets {
        let deanomization_path = path;
        let deanomization_vec =
            deanonymized_users_over_time(&source_relationship_anonymity_sets, &network_trace);
        fs::write(
            deanomization_path,
            serde_json::to_string_pretty(&deanomization_vec)?,
        )?;
    }

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
