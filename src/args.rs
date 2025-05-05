// use crate::workload::Workload;
use crate::{corpus::Corpus, db::Backend};
use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// CLI argument parse
#[derive(Clone, Parser, Debug, Serialize)]
#[command(author = "marvin-j97", version = env!("CARGO_PKG_VERSION"), about = "Rust KV-store profiler")]
#[command(propagate_version = true)]
pub struct Args {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Clone, Debug, Eq, PartialEq, clap::ValueEnum, Serialize, Deserialize)]
pub enum LsmCompaction {
    Leveled,
    Tiered,
}

#[derive(Clone, Debug, Eq, PartialEq, clap::ValueEnum, Serialize, Deserialize)]
pub enum Compression {
    None,
    Lz4,
}

impl std::fmt::Display for LsmCompaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Leveled => "LCS",
                Self::Tiered => "STCS",
            }
        )
    }
}

#[derive(Parser, Clone, Debug, Serialize)]
#[clap(rename_all = "kebab_case")]
pub struct CommonRunOptions {
    /// Granularity in milliseconds with which to poll metrics
    #[arg(long, alias = "granularity", default_value_t = 500)]
    pub granularity_ms: u16,

    /// Database to use
    #[arg(long, value_enum)]
    pub backend: Backend,

    /// Where to store temporary database
    #[arg(long)]
    pub data_dir: PathBuf,

    /// Where to store result jsonl file
    #[arg(long)]
    pub out: Option<PathBuf>,

    /// Display name
    ///
    /// The default is the DB name + version
    #[arg(long)]
    pub display_name: Option<String>,

    /// How many seconds to run the workload for
    #[arg(long, default_value_t = 60)]
    pub seconds: u16,

    /// Use immediately durable writes (synchronous writes)
    #[arg(long, alias = "sync", default_value_t = false)]
    pub fsync: bool,

    #[arg(long, default_value_t = 512_000_000)] // 512 MB
    pub cache_size: u64,

    /// Compression to use, if supported
    #[arg(long, value_enum, default_value_t = Compression::Lz4)]
    pub compression: Compression,

    /// Compaction for LSM-trees
    #[arg(long, value_enum, default_value_t = LsmCompaction::Leveled)]
    pub lsm_compaction: LsmCompaction,
    /*
    /// Number of threads to use. Not applicable to all workloads
    #[arg(long, default_value_t = 1)]
    pub threads: usize,

    /// Whether to use random or monotonic keys. Not applicable to all workloads
    #[arg(long, default_value_t = false)]
    pub write_random: bool,

    #[arg(long, default_value_t = false)]
    pub warmup_cache: bool,

    /// Compaction for LSM-trees
    #[arg(long, value_enum, default_value_t = LsmCompaction::Leveled)]
    pub lsm_compaction: LsmCompaction, */
    // #[arg(long)]
    // pub key_size: u8,

    // /// Use KV-separation
    // #[arg(long, alias = "lsm_kv_sep", default_value_t = false)]
    // pub lsm_kv_separation: bool,

    // /// Block size for LSM-trees
    // #[arg(long, default_value_t = 4_096)]
    // pub lsm_block_size: u16,

    // /// Intermittenly flush sled to keep memory usage sane
    // /// This is hopefully a temporary workaround
    // #[arg(long, default_value_t = false)]
    // pub sled_flush: bool,
}

#[derive(Parser, Clone, Debug, Serialize)]
#[clap(rename_all = "kebab_case")]
pub struct ReportOptions {
    /// Input files
    pub files: Vec<PathBuf>,

    /// Output file
    #[arg(short = 'o', long = "out", default_value = "report.html")]
    pub out: PathBuf,
}

#[derive(Clone, Debug, ClapArgs, Serialize)]
pub struct RunArgs {
    #[command(flatten)]
    pub args: CommonRunOptions,

    #[command(subcommand)]
    pub workload: Workload,
}

#[derive(Copy, Eq, PartialEq, Debug, Clone, ValueEnum, Serialize, Deserialize)]
pub enum YcsbType {
    A,
    B,
    C,
}

impl std::fmt::Display for YcsbType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::A => "Ycsb A",
                Self::B => "Ycsb B",
                Self::C => "Ycsb C",
            }
        )
    }
}

#[derive(Parser, Clone, Debug, Serialize)]
pub struct YcsbOptions {
    #[arg(long = "type")]
    pub r#type: YcsbType,

    #[arg(long, value_enum, default_value_t = Corpus::Random)]
    pub corpus: Corpus,

    #[arg(long, default_value_t = 1_000_000)]
    pub item_count: usize,

    #[arg(long, default_value_t = 1.0)]
    pub zipf_exponent: f64,

    #[arg(long, default_value_t = 200)]
    pub value_size: u32,

    /// Whether to use random or Zipfian read distribution. Not applicable to all workloads
    #[arg(long, default_value_t = false)]
    pub read_random: bool,
}

#[derive(Parser, Clone, Debug, Serialize)]
pub struct QueueOptions {
    #[arg(long, default_value_t = true)]
    pub backpressure: bool,

    #[arg(long, default_value_t = 1_000)]
    pub max_pending: u64,

    #[arg(long, value_enum, default_value_t = Corpus::Random)]
    pub corpus: Corpus,

    #[arg(long, default_value_t = 200)]
    pub value_size: u32,
}

#[derive(Clone, Debug, Subcommand, Serialize)]
pub enum Workload {
    Ycsb(YcsbOptions),
    Queue(QueueOptions),
}

#[derive(Clone, Subcommand, Debug, Serialize)]
pub enum Commands {
    Run(RunArgs),
    Report(ReportOptions),
}
