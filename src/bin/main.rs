use clap::{Parser, Subcommand};
use mirage::{apply, revert};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Apply deduplication to target directory
    Apply {
        /// Target directory path
        #[arg(default_value = ".")]
        path: String,
    },

    Revert {
        /// Target directory path
        #[arg(default_value = ".")]
        path: String,
    },
}

fn main() {
    pretty_env_logger::init();
    let cli = Cli::parse();

    match &cli.command {
        Commands::Apply { path } => {
            println!("Applying deduplication to path: {}", path);
            apply(path).unwrap_or_else(|err| {
                eprintln!("Error applying deduplication: {}", err);
                std::process::exit(1);
            });
        }
        Commands::Revert { path } => {
            println!("Reverting deduplication to path: {}", path);
            revert(path).unwrap_or_else(|err| {
                eprintln!("Error reverting deduplication: {}", err);
                std::process::exit(1);
            });
        }
    }
}
