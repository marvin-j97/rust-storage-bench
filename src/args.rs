use crate::db::Backend;
use crate::workload::Workload;
use clap::{Parser, Subcommand};
use serde::Serialize;
use std::path::PathBuf;

/// CLI argument parse
#[derive(Clone, Parser, Debug, Serialize)]
#[command(author = "marvin-j97", version = env!("CARGO_PKG_VERSION"), about = "Rust KV-store profiler")]
#[command(propagate_version = true)]
pub struct Args {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Parser, Clone, Debug, Serialize)]
#[clap(rename_all = "kebab_case")]
pub struct RunOptions {
    #[arg(long, value_enum)]
    pub backend: Backend,

    #[arg(long)]
    pub data_dir: PathBuf,

    #[arg(long)]
    pub out: PathBuf,

    #[arg(long)]
    pub display_name: Option<String>,

    #[arg(long, value_enum)]
    pub workload: Workload,

    #[arg(long, default_value_t = 1)]
    pub minutes: u16,

    #[arg(long, alias = "granularity", default_value_t = 500)]
    pub granularity_ms: u16,

    #[arg(long, default_value_t = 16_000_000)]
    pub cache_size: u64,
    // #[arg(long, default_value_t = 1)]
    // pub threads: u8,

    // #[arg(long, default_value_t = 0)]
    // pub items: u32,

    // #[arg(long)]
    // pub key_size: u8,

    // #[arg(long)]
    // pub value_size: u32,

    // /// Use KV-separation
    // #[arg(long, alias = "lsm_kv_sep", default_value_t = false)]
    // pub lsm_kv_separation: bool,

    // /// Block size for LSM-trees
    // #[arg(long, default_value_t = 4_096)]
    // pub lsm_block_size: u16,

    // /// Compaction for LSM-trees
    // #[arg(long, value_enum, default_value_t = LsmCompaction::Leveled)]
    // pub lsm_compaction: LsmCompaction,

    // /// Compression for LSM-trees
    // #[arg(long, value_enum, default_value_t = Compression::Lz4)]
    // pub lsm_compression: Compression,

    // /// Intermittenly flush sled to keep memory usage sane
    // /// This is hopefully a temporary workaround
    // #[arg(long, default_value_t = false)]
    // pub sled_flush: bool,

    // #[arg(long, default_value_t = false)]
    // pub fsync: bool,
}

#[derive(Parser, Clone, Debug, Serialize)]
#[clap(rename_all = "kebab_case")]
pub struct ReportOptions {
    /// Input files
    pub files: Vec<PathBuf>,

    /// Output file
    #[arg(short = 'o', long = "out", default_value = "out.html")]
    pub out: PathBuf,
}

#[derive(Clone, Subcommand, Debug, Serialize)]
pub enum Commands {
    Run(RunOptions),
    Report(ReportOptions),
}
