use crate::workload::{
    event_log::Options as EventLogOptions, feed::Options as FeedOptions,
    timeseries::Options as TimeSeriesOptions, webtable::Options as WebtableOptions,
};
use crate::workload::{queue::Options as QueueOptions, ycsb::Options as YcsbOptions};
use crate::{corpus::Corpus, db::Backend};
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
    #[arg(long, value_enum)]
    pub marker_shape: Option<MarkerShape>,

    /// How many seconds to run the workload for
    #[arg(long, default_value_t = 60)]
    pub seconds: u32,

    /// How much max data to store (to avoid out-of-space)
    ///
    /// Default: 1 TB
    #[arg(long, default_value_t = 1_000_000_000_000)]
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

    /// Block size for LSM-trees
    #[arg(long, default_value_t = 4_096)]
    pub lsm_block_size: u32,

    /// Bloom filter BPK
    #[arg(long, default_value_t = 10)]
    pub lsm_bloom_bpk: u8,

    #[arg(long, default_value_t = 0.0)]
    pub lsm_data_block_hash_ratio: f32,
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

#[derive(Parser, Clone, Debug, Serialize)]
pub struct ReadWriteOptions {
    #[arg(long, default_value_t = 1_000_000)]
    pub item_count: usize,

    #[arg(long, default_value_t = false)]
    pub write_only: bool,

    #[arg(long, default_value_t = true)]
    pub write_random: bool,

    #[arg(long, default_value_t = false)]
    pub read_random: bool,

    #[arg(long, value_enum, default_value_t = Corpus::Random)]
    pub corpus: Corpus,

    #[arg(long, default_value_t = 200)]
    pub value_size: u32,

    #[arg(long, default_value_t = 1.0)]
    pub zipf_exponent: f64,
}

#[derive(Clone, Debug, Subcommand, Serialize)]
pub enum Workload {
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
