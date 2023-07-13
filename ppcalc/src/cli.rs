use std::{env, fs::File, io::BufReader, path::Path};

use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use crate::destination;

#[derive(Serialize, Deserialize, Builder)]
pub struct Parameters {
    pub reuse_sources: bool,
    pub experiment: String,
    pub destination_selection_type: destination::DestinationSelectionType,
    pub num_sources: u64,
    pub num_destinations: u64,
    pub source_imd_mean: f64,
    pub source_imd_dev: f64,
    pub source_wait_mean: f64,
    pub source_wait_dev: f64,
    pub num_messages_mean: f64,
    pub num_messages_dev: f64,
    pub network_delay_min: i64,
    pub network_delay_max: i64,
    pub testcase_path: Option<String>,
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
            testcase_path: None,
        }
    }
}
fn help() {
    println!("Help is currently not available. Please panic");
}

pub fn cli_parsing(mut args: env::Args) -> Result<Parameters, ParametersBuilderError> {
    let mut params = ParametersBuilder::create_empty();
    let mut use_default_values = false;
    while let Some(arg) = args.next() {
        match &arg[..] {
            "-h" | "--help" => help(),
            "-r" => {
                params.reuse_sources(true);
            }
            "-s" | "--sources" => {
                if let Some(arg_config) = args.next() {
                    params.num_sources(arg_config.parse().unwrap());
                } else {
                    panic!("No value specified for parameter -s");
                }
            }
            "-d" | "--destinations" => {
                if let Some(arg_config) = args.next() {
                    params.num_destinations(arg_config.parse().unwrap());
                } else {
                    panic!("No value specified for parameter -d");
                }
            }
            "--default" => {
                use_default_values = true;
            }
            "--destination_selection" => {
                if let Some(arg_config) = args.next() {
                    match arg_config.as_str() {
                        "normal" => {
                            params.destination_selection_type(
                                destination::DestinationSelectionType::Normal,
                            );
                        }
                        "uniform" => {
                            params.destination_selection_type(
                                destination::DestinationSelectionType::Uniform,
                            );
                        }
                        "roundrobin" => {
                            params.destination_selection_type(
                                destination::DestinationSelectionType::RoundRobin,
                            );
                        }
                        "smallworld" => {
                            params.destination_selection_type(
                                destination::DestinationSelectionType::SmallWorld,
                            );
                        }
                        _ => {
                            panic!("Wrong argument for --destination_selection");
                        }
                    }
                } else {
                    panic!("No value specified for parameter --destination_selection");
                }
            }
            "-e" | "--experiment" => {
                if let Some(arg_config) = args.next() {
                    params.experiment(arg_config.parse().unwrap());
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
                            params.source_imd_mean(arg_source_mean.parse().unwrap());
                            params.source_imd_dev(arg_source_dev.parse().unwrap());
                        }
                        _ => {
                            panic!("Wrong argument for --source_imd_distr");
                        }
                    }
                } else {
                    panic!("No value specified for parameter --source_imd_distr.");
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
                            params.source_wait_mean(arg_source_wait_mean.parse().unwrap());
                            params.source_wait_dev(arg_source_wait_dev.parse().unwrap());
                        }
                        _ => {
                            panic!("Wrong argument for --source_wait_distr");
                        }
                    }
                } else {
                    panic!("No value specified for parameter --source_wait_distr.");
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
                            params.num_messages_mean(arg_num_messages_mean.parse().unwrap());
                            params.num_messages_dev(arg_num_messages_dev.parse().unwrap());
                        }
                        _ => {
                            panic!("Wrong argument for --num_messages_distr");
                        }
                    }
                } else {
                    panic!("No value specified for parameter --num_messages_distr.");
                }
            }
            "--generate-testcase" => {
                if let Some(arg_config) = args.next() {
                    params.testcase_path(Some(arg_config.parse().unwrap()));
                } else {
                    panic!("No value specified for parameter --generate-testcase <Path>");
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
                            params.network_delay_min(arg_network_delay_min.parse().unwrap());
                            params.network_delay_max(arg_network_delay_max.parse().unwrap());
                        }
                        _ => {
                            panic!("Wrong argument for --network_delay");
                        }
                    }
                } else {
                    panic!("No value specified for parameter --network_delay.");
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
    if use_default_values {
        params.reuse_sources(params.reuse_sources.unwrap_or_default().clone());
        params.destination_selection_type(
            params
                .destination_selection_type
                .clone()
                .unwrap_or_default(),
        );
        params.num_sources(params.num_sources.unwrap_or_default());
        params.num_destinations(
            params
                .num_destinations
                .unwrap_or(Parameters::default().num_destinations),
        );
        params.source_imd_mean(
            params
                .source_imd_mean
                .unwrap_or(Parameters::default().source_imd_mean),
        );
        params.source_imd_dev(
            params
                .source_imd_dev
                .unwrap_or(Parameters::default().source_imd_dev),
        );
        params.source_wait_mean(
            params
                .source_wait_mean
                .unwrap_or(Parameters::default().source_wait_mean),
        );
        params.source_wait_dev(
            params
                .source_wait_dev
                .unwrap_or(Parameters::default().source_wait_dev),
        );
        params.num_messages_mean(
            params
                .num_messages_mean
                .unwrap_or(Parameters::default().num_messages_mean),
        );
        params.num_messages_dev(
            params
                .num_messages_dev
                .unwrap_or(Parameters::default().num_messages_dev),
        );
        params.network_delay_min(
            params
                .network_delay_min
                .unwrap_or(Parameters::default().network_delay_min),
        );
        params.network_delay_max(
            params
                .network_delay_max
                .unwrap_or(Parameters::default().network_delay_max),
        );
        params.experiment(
            params
                .experiment
                .clone()
                .unwrap_or(Parameters::default().experiment),
        );
    }
    params.build()
}

pub fn write_params(params: &Parameters, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let wtr = std::fs::File::create(path)?;
    serde_json::to_writer_pretty(&wtr, params)?;
    Ok(())
}

pub fn read_params(path: &str) -> Result<Parameters, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    // Read the JSON contents of the file as an instance of `User`.
    let message_anon_set = serde_json::from_reader(reader)?;
    Ok(message_anon_set)
}
