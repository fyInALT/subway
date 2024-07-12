use clap::{Parser, Subcommand};
use std::path::PathBuf;

use crate::build_info;

#[derive(Parser, Debug)]
#[command(version = build_info::GIT_VERSION, about)]
pub struct Cli {
    /// The config file to use
    #[arg(short, long, default_value = "configs/config.yml")]
    pub config: PathBuf,

    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    Validate,
}

pub fn parse_args() -> Cli {
    Cli::parse()
}

impl Cli {
    pub fn is_validate(&self) -> bool {
        matches!(self.command, Some(Command::Validate))
    }
}
