use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

use crate::destination::DestinationSelectionType;

/// Tool to analyze and generate network traces of anonymity networks.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Generate a new trace file, simulating ACN communication
    Generate(GenerateArgs),
    /// Analyze a network trace, estimating the achieved anonymity
    Analyze(AnalyzeArgs),
}

#[derive(Args, Debug)]
pub struct AnalyzeArgs {
    /// Minimum window for anonymity metric (milliseconds)
    #[arg(long)]
    pub min_window: u64,

    /// Maximum window for anonymity metric (milliseconds)
    #[arg(long)]
    pub max_window: u64,

    /// Output the analysis data as a testcase
    #[arg(long, value_name = "TESTCASE_FOLDER")]
    pub generate_testcase: Option<String>,

    /// Output the times when (and if) users were de-anonymized
    #[arg(long, value_name = "OUTFILE_FILE")]
    pub output_user_anonsets: Option<PathBuf>,

    /// Output JSON file containing the computed anonymity sets (or their sizes) per source message.
    /// If the file name ends in ".zst", it is compressed with zstandard.
    #[arg(long, short, value_name = "OUT_FILE")]
    pub output: Option<PathBuf>,

    /// Output only the size of the anonymity sets. This option cannot be used when testcases are generated.
    #[arg(long, default_value = "false", value_name = "OUT_FILE", conflicts_with_all = ["generate_testcase", "output_user_anonsets"])]
    pub sizes_only: bool,

    /// Input CSV trace file to analyze
    #[arg(value_name = "TRACE_FILE")]
    pub input: PathBuf,
}

#[derive(Args, Debug)]
pub struct GenerateArgs {
    /// Number of sources to send from
    #[arg(short = 's', long = "sources")]
    pub num_sources: u64,
    /// Number of destinations to send to
    #[arg(short = 'd', long = "destinations")]
    pub num_destinations: u64,
    /// Reuse the sources from the specified trace file
    #[arg(long, value_name = "TRACE_FILE")]
    pub reuse_sources: Option<PathBuf>,
    #[arg(long, value_name = "uniform|roundrobin|normal", value_parser = parse_destination_selection_type)]
    pub destination_selection_type: DestinationSelectionType,
    #[arg(long)]
    pub source_imd_mean: f64,
    #[arg(long)]
    pub source_imd_dev: f64,
    #[arg(long)]
    pub source_wait_mean: f64,
    #[arg(long)]
    pub source_wait_dev: f64,
    #[arg(long)]
    pub num_messages_mean: f64,
    #[arg(long)]
    pub num_messages_dev: f64,
    #[arg(long)]
    pub network_delay_min: i64,
    #[arg(long)]
    pub network_delay_max: i64,

    /// Output CSV file to save the trace to
    #[arg(value_name = "OUTPUT_FILE")]
    pub output: PathBuf,
}

impl Cli {
    pub fn parse() -> Cli {
        <Cli as Parser>::parse()
    }
}

fn parse_destination_selection_type(s: &str) -> Result<DestinationSelectionType, String> {
    match s {
        "normal" => Ok(DestinationSelectionType::Normal),
        "uniform" => Ok(DestinationSelectionType::Uniform),
        "roundrobin" => Ok(DestinationSelectionType::RoundRobin),
        _ => Err(format!("Invalid destination selection type \"{}\".", s)),
    }
}
