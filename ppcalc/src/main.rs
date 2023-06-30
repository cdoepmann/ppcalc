mod analyze;
mod bench;
mod cli;
mod destination;
mod generate;
mod network;
mod plot;
mod source;
mod trace;

use cli::Cli;

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        cli::Commands::Generate(args) => {
            generate::run(args)?;
        }
        cli::Commands::Analyze(args) => {
            analyze::run(args)?;
        }
    }

    Ok(())
}
