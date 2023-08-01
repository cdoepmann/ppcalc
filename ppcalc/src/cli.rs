use std::marker::PhantomData;
use std::path::PathBuf;
use std::str::FromStr;

use clap::{Args, Parser, Subcommand};
use rand::distributions::{uniform::SampleUniform, Distribution, Uniform};
use rand_distr::Normal;

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

    /// Assignment strategy for connecting sources to destinations
    #[arg(long, value_name = "uniform|roundrobin|normal", value_parser = parse_destination_selection_type)]
    pub destination_selection_type: DestinationSelectionType,

    /// Probability distribution for the inter-message delay
    #[arg(long, value_name = "DISTRIBUTION", value_parser = parse_distribution::<f64>)]
    pub source_imd: ParsedDistribution<f64>,

    /// Probability distribution for the time the source waits before sending
    #[arg(long, value_name = "DISTRIBUTION", value_parser = parse_distribution::<f64>)]
    pub source_wait: ParsedDistribution<f64>,

    /// Probability distribution for the number of messages per user
    #[arg(long, value_name = "DISTRIBUTION", value_parser = parse_distribution::<u64>)]
    pub num_messages: ParsedDistribution<u64>,

    /// Probability distribution for the network delay [ms]
    #[arg(long, value_name = "DISTRIBUTION", value_parser = parse_distribution::<u64>)]
    pub network_delay: ParsedDistribution<u64>,

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

/// A `Distribution` equivalent that is object-safe.
///
/// See [https://stackoverflow.com/a/75007203] for source and explanation.
pub trait ErasedDistribution<T> {
    fn sample(&self, rng: &mut dyn rand::RngCore) -> T;
}

impl<T, D: Distribution<T> + ?Sized> ErasedDistribution<T> for D {
    fn sample(&self, rng: &mut dyn rand::RngCore) -> T {
        <Self as Distribution<T>>::sample(self, rng)
    }
}

impl<T> Distribution<T> for dyn ErasedDistribution<T> {
    fn sample<R: rand::Rng + ?Sized>(&self, mut rng: &mut R) -> T {
        <dyn ErasedDistribution<T> as ErasedDistribution<T>>::sample(self, &mut rng)
    }
}

impl<T> Distribution<T> for &'_ dyn ErasedDistribution<T> {
    fn sample<R: rand::Rng + ?Sized>(&self, mut rng: &mut R) -> T {
        <dyn ErasedDistribution<T> as ErasedDistribution<T>>::sample(&**self, &mut rng)
    }
}

/// A constant value fit into a `Distribution`.
#[derive(Clone)]
pub struct Constant<T> {
    value: T,
}

impl<T> Constant<T> {
    fn new(value: T) -> Constant<T> {
        Constant { value }
    }
}

impl<T: Clone> Distribution<T> for Constant<T> {
    fn sample<R: rand::Rng + ?Sized>(&self, _rng: &mut R) -> T {
        self.value.clone()
    }
}

/// A normal distribution of float OR values.
///
/// Under the hood, a f64-based normal distribution is used. Integer values are
/// obtained by ceiling.
#[derive(Clone)]
pub struct NormalAllowingIntegers<T: SampledValue> {
    float_distribution: Normal<f64>,
    phantom: PhantomData<T>,
}

impl<T: SampledValue> NormalAllowingIntegers<T> {
    fn new(mean: f64, dev: f64) -> Result<Self, rand_distr::NormalError> {
        Ok(NormalAllowingIntegers {
            float_distribution: Normal::new(mean, dev)?,
            phantom: PhantomData,
        })
    }
}

impl<T: SampledValue> Distribution<T> for NormalAllowingIntegers<T> {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> T {
        T::from_f64(rand_distr::Distribution::sample(
            &self.float_distribution,
            rng,
        ))
    }
}

/// A type that can be used as a result type from sampling any of our
/// dynamically built distributions. It must allow to be (lossily) built from
/// a sampled f64 value.
pub trait SampledValue: FromStr + Clone + SampleUniform {
    fn from_f64(value: f64) -> Self;
}

impl SampledValue for f64 {
    fn from_f64(value: f64) -> Self {
        value
    }
}

impl SampledValue for u64 {
    fn from_f64(value: f64) -> Self {
        value.ceil() as u64
    }
}

impl SampledValue for i64 {
    fn from_f64(value: f64) -> Self {
        value.ceil() as i64
    }
}

fn parse_distribution<T: SampledValue>(s: &str) -> Result<ParsedDistribution<T>, String> {
    // common error
    let err = || {
        "Invalid distribution. Specify it using one of the following forms:
    constant:VALUE
    uniform:MIN:MAX
    normal:mean:dev"
            .to_string()
    };

    let splitted: Vec<_> = s.split(':').collect();

    match splitted[..] {
        ["constant", value] => {
            let value: T = value.parse::<T>().map_err(|_| err())?;
            Ok(ParsedDistribution::Constant { value })
        }
        ["uniform", min, max] => {
            let min: T = min.parse::<T>().map_err(|_| err())?;
            let max: T = max.parse::<T>().map_err(|_| err())?;
            Ok(ParsedDistribution::Uniform { min, max })
        }
        ["normal", mean, dev] => {
            let mean = mean.parse::<f64>().map_err(|_| err())?;
            let dev = dev.parse::<f64>().map_err(|_| err())?;
            Ok(ParsedDistribution::Normal { mean, dev })
        }
        _ => return Err(err()),
    }
}

/// A set of parsed parameters for a probability distribution
#[derive(Debug, Clone)]
pub enum ParsedDistribution<T: SampledValue + 'static> {
    Constant { value: T },
    Uniform { min: T, max: T },
    Normal { mean: f64, dev: f64 },
}

impl<T: SampledValue + Copy + 'static> ParsedDistribution<T> {
    pub fn make_distr(
        &self,
    ) -> Result<Box<dyn ErasedDistribution<T>>, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Self::Constant { value } => Ok(Box::new(Constant::new(*value))),
            Self::Uniform { min, max } => Ok(Box::new(Uniform::new_inclusive(*min, *max))),
            Self::Normal { mean, dev } => Ok(Box::new(
                NormalAllowingIntegers::new(*mean, *dev)
                    .map_err(|e| format!("Error building normal distribution: {}", e))?,
            )),
        }
    }
}
