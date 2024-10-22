use std::fs;
use std::io::Write;
use std::path::Path;

use anyhow::anyhow;
use fxhash::FxHashMap as HashMap;
use serde_json;
use serde_json::json;
use time::Duration;
use zstd;

use ppcalc_metric::{DestinationId, MessageId, SourceId, Trace, TraceBuilder};

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
            output_anonymity_sets(path, &source_relationship_anonymity_sets, &network_trace)?;
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
            output_anonymity_sets(path, &source_relationship_anonymity_sets, &network_trace)?;
        }
    }

    Ok(())
}

trait JsonAnonymitySet {
    fn format_anonymity_set(&self) -> serde_json::Value;

    fn size(&self) -> usize;
}

impl JsonAnonymitySet for Vec<DestinationId> {
    fn format_anonymity_set(&self) -> serde_json::Value {
        json!(self.iter().map(|x| x.to_num()).collect::<Vec<_>>())
    }

    fn size(&self) -> usize {
        self.len()
    }
}

impl JsonAnonymitySet for usize {
    fn format_anonymity_set(&self) -> serde_json::Value {
        json!(self)
    }

    fn size(&self) -> usize {
        *self
    }
}

fn output_anonymity_sets<T: JsonAnonymitySet>(
    path: impl AsRef<Path>,
    anonymity_sets: &HashMap<SourceId, Vec<(MessageId, T)>>,
    trace: &Trace,
) -> anyhow::Result<()> {
    use serde_json::{Map, Value};
    let path = path.as_ref();

    let sets_per_user: Map<String, Value> = anonymity_sets
        .iter()
        .map(|(k, v)| {
            let msgs = Value::Array(
                v.iter()
                    .map(|(msgid, anonset)| {
                        json!({
                            "m": msgid,
                            "as": anonset.format_anonymity_set()
                        })
                    })
                    .collect(),
            );

            let last_anonset_size = v.last().map(|x| x.1.size());
            let deanonymized_at_index = {
                let index = v.partition_point(|(_msgid, anonset)| anonset.size() > 1);
                if index >= v.len() {
                    None
                } else {
                    Some(index)
                }
            };
            let time_to_deanonymization = deanonymized_at_index
                .map(|deanon_index| {
                    trace.message_sent(&v[deanon_index].0).unwrap()
                        - trace.message_sent(&v[0].0).unwrap()
                })
                .map(|duration| duration.as_seconds_f32());

            (
                k.to_string(),
                json!({
                    "last_anonset_size": last_anonset_size,
                    "deanonymized_at_num": deanonymized_at_index,
                    "time_to_deanon": time_to_deanonymization,
                    "msgs": msgs
                }),
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
