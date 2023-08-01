use std::fs;
use std::io::Write;
use std::path::Path;

use anyhow::anyhow;
use fxhash::FxHashMap as HashMap;
use serde_json;
use serde_json::json;
use time::Duration;
use zstd;

use ppcalc_metric::{DestinationId, MessageId, SourceId, TraceBuilder};

use crate::cli::AnalyzeArgs;
use crate::plot::deanonymized_users_over_time;

pub fn run(args: AnalyzeArgs) -> anyhow::Result<()> {
    // load trace
    let network_trace = TraceBuilder::from_csv(&args.input)
        .map_err(|e| anyhow!(e))?
        .build()?;

    if !args.sizes_only {
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

        if let Some(path) = args.output {
            output_anonymity_sets(path, &source_relationship_anonymity_sets)?;
        }

        if let Some(path) = args.generate_testcase {
            ppcalc_metric::simple_example_generator(
                args.min_window as i64,
                args.max_window as i64,
                &network_trace,
                source_relationship_anonymity_sets,
                path.into(),
            )
            .map_err(|e| anyhow!(e))?;
        }
    } else {
        // sizes only
        let (source_relationship_anonymity_sets, _destination_relationship_anonymity_sets) =
            ppcalc_metric::compute_relationship_anonymity_sizes(
                &network_trace,
                Duration::milliseconds(args.min_window as i64),
                Duration::milliseconds(args.max_window as i64),
            )
            .map_err(|e| anyhow!(e))?;

        if let Some(path) = args.output {
            output_anonymity_sets(path, &source_relationship_anonymity_sets)?;
        }
    }

    Ok(())
}

trait JsonAnonymitySet {
    fn format_anonymity_set(anonymity_set: &Self) -> serde_json::Value;
}

impl JsonAnonymitySet for Vec<DestinationId> {
    fn format_anonymity_set(anonymity_set: &Self) -> serde_json::Value {
        json!(anonymity_set.iter().map(|x| x.to_num()).collect::<Vec<_>>())
    }
}

impl JsonAnonymitySet for usize {
    fn format_anonymity_set(anonymity_set: &Self) -> serde_json::Value {
        json!(anonymity_set)
    }
}

fn output_anonymity_sets<T: JsonAnonymitySet>(
    path: impl AsRef<Path>,
    anonymity_sets: &HashMap<SourceId, Vec<(MessageId, T)>>,
) -> anyhow::Result<()> {
    use serde_json::{Map, Value};
    let path = path.as_ref();

    let sets_per_user: Map<String, Value> = anonymity_sets
        .iter()
        .map(|(k, v)| {
            (
                k.to_string(),
                Value::Array(
                    v.iter()
                        .map(|(msgid, anonset)| {
                            json!({
                                "m": msgid,
                                "as": T::format_anonymity_set(anonset)
                            })
                        })
                        .collect(),
                ),
            )
        })
        .collect();

    let mut file_writer: Box<dyn Write> = {
        let file = fs::File::create(path)?;

        if path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .ends_with(".zst")
        {
            Box::new(zstd::Encoder::new(file, 16)?.auto_finish())
        } else {
            Box::new(file)
        }
    };

    serde_json::to_writer_pretty(&mut file_writer, &sets_per_user)?;

    Ok(())
}
