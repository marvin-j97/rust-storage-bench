use crate::db::Backend;
use crate::workload::{
    column_store::Options as ColumnStoreOptions, event_log::Options as EventLogOptions,
    feed::Options as FeedOptions, read_write::Options as ReadWriteOptions,
    timeseries::Options as TimeSeriesOptions, webtable::Options as WebtableOptions,
};
use crate::workload::{queue::Options as QueueOptions, ycsb::Options as YcsbOptions};
use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// CLI argument parse
#[derive(Clone, Parser, Debug, Serialize)]
#[command(author = "marvin-j97", version = env!("CARGO_PKG_VERSION"), about = "Rust storage engine benchmark")]
#[command(propagate_version = true)]
pub struct Args {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Clone, Debug, Eq, PartialEq, clap::ValueEnum, Serialize, Deserialize)]
pub enum LsmCompaction {
    Leveled,
    Tiered,
    Fifo,
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
                Self::Fifo => "FIFO",
            }
        )
    }
}

// TODO: actually implement in frontend
/// Apexcharts marker style
///
/// https://apexcharts.com/docs/options/markers/
#[derive(Copy, Clone, Debug, Deserialize, Serialize, ValueEnum)]
pub enum MarkerShape {
    #[serde(rename = "circle")]
    Circle,

    #[serde(rename = "square")]
    Square,

    #[serde(rename = "line")]
    Line,

    #[serde(rename = "plus")]
    Plus,

    #[serde(rename = "cross")]
    Cross,

    #[serde(rename = "star")]
    Star,

    #[serde(rename = "sparkle")]
    Sparkle,

    #[serde(rename = "diamond")]
    Diamond,

    #[serde(rename = "triangle")]
    Triangle,
}

#[derive(Parser, Clone, Debug, Serialize)]
#[clap(rename_all = "kebab_case")]
pub struct CommonRunOptions {
    /// Benchmark ID
    ///
    /// May be used identification in further processing
    #[arg(long, default_value_t = scru128::new_string())]
    pub id: String,

    #[arg(long, default_value_t = String::from("Untitled workload"))]
    pub title: String,

    #[arg(long, default_value_t = String::from(""))]
    pub description: String,

    /// Granularity in milliseconds with which to poll metrics
    #[arg(long, alias = "granularity", default_value_t = 1_000)]
    pub granularity_ms: u32,

    #[arg(long, alias = "auto-granularity")]
    pub auto_granularity: Option<u32>,

    /// Database to use
    #[arg(long, value_enum)]
    pub backend: Backend,

    /// Where to store temporary database
    #[arg(long)]
    pub data_dir: PathBuf,

    #[arg(long, default_value_t = true)]
    #[arg(long = "no-clean-data-dir", action = clap::ArgAction::SetFalse)]
    pub clean_data_dir: bool,

    /// Where to store result jsonl file
    #[arg(long)]
    pub out: Option<PathBuf>,

    /// Display name
    ///
    /// The default is the database backend name + version
    #[arg(long)]
    pub display_name: Option<String>,

    /// Custom display color
    ///
    /// Should be hex format: #RRGGBB
    #[arg(long, alias = "colour")]
    pub color: Option<String>,

    /// Custom line graph marker shape
    #[arg(long, value_enum, alias = "marker", alias = "markers")]
    pub marker_shape: Option<MarkerShape>,

    /// How many seconds to run the workload for
    #[arg(long, default_value_t = 60)]
    pub seconds: u32,

    /// How much max data to store (to avoid out-of-space)
    ///
    /// Default: 500 GB
    #[arg(long, default_value_t = 500_000_000_000)]
    pub max_data_bytes: u64,

    /// Use immediately durable writes (synchronous writes)
    #[arg(long, alias = "sync", default_value_t = false)]
    pub fsync: bool,

    #[arg(long, default_value_t = 512_000_000)] // 512 MB
    pub cache_size: u64,

    /// Compression to use, if supported
    #[arg(long, value_enum, default_value_t = Compression::Lz4)]
    pub compression: Compression,

    /// Block size for LSM-trees
    #[arg(long, default_value_t = 64_000_000)]
    pub lsm_write_buffer_bytes: u64,

    /// Compaction for LSM-trees
    #[arg(long, value_enum, default_value_t = LsmCompaction::Leveled)]
    pub lsm_compaction: LsmCompaction,

    #[arg(long, default_value_t = 512_000_000)]
    pub lsm_fifo_limit_bytes: u64,

    #[arg(long, default_value_t = 4)]
    pub lsm_workers: usize,

    /// Block size for LSM-trees
    #[arg(long, default_value_t = 4_096)]
    pub lsm_block_size: u32,

    /// Bloom filter BPK
    #[arg(long)]
    pub lsm_bloom_bpk: Option<u8>,

    #[arg(long)]
    pub lsm_data_block_hash_ratio: Option<f32>,

    /// Forces KV-separation
    #[arg(long, default_value_t = false)]
    pub lsm_kv_separation: bool,

    #[arg(long, default_value_t = false)]
    pub lsm_use_partitioned_meta: bool,

    #[arg(long, default_value_t = false)]
    pub lsm_pin_all_meta: bool,

    #[arg(long, value_enum, default_value_t = Compression::None)]
    pub journal_compression: Compression,

    #[arg(long, default_value_t = 4)]
    pub lsm_l0_threshold: usize,
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

#[derive(Clone, Debug, Subcommand, Serialize)]
pub enum Workload {
    Idle,

    ColumnStore(ColumnStoreOptions),

    Feed(FeedOptions),

    EventLog(EventLogOptions),

    Webtable(WebtableOptions),

    TimeSeries(TimeSeriesOptions),

    // TpcC,
    Ycsb(YcsbOptions),

    /// Runs a queue-like workload where:
    ///
    /// - one writer writes into a queue (using a global sequence number as key)
    ///
    /// - one reader reads from the queue
    ///
    /// The queue can optionally be configured with backpressure (such that the queue never exceeds a certain size),
    /// which can throttle writes.
    Queue(QueueOptions),

    ReadWrite(ReadWriteOptions),
}

#[derive(Parser, Clone, Debug, Serialize)]
#[clap(rename_all = "kebab_case")]
pub struct AggregateOptions {
    /// Input files
    pub files: Vec<PathBuf>,

    /// Columns to project
    #[arg(long = "project")]
    pub project: Vec<String>,

    /// Output file
    #[arg(short = 'o', long = "out", default_value = "aggregate.jsonl")]
    pub out: PathBuf,
}

#[derive(Clone, Subcommand, Debug, Serialize)]
pub enum Commands {
    Run(RunArgs),
    Report(ReportOptions),
    Aggregate(AggregateOptions),
}
