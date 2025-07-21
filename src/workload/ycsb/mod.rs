use crate::corpus::Corpus;
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};

pub mod a;
pub mod b;
pub mod c;

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
pub struct Options {
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

    /// Whether to use random or Zipfian read distribution.
    #[arg(long, default_value_t = false)]
    pub read_random: bool,
}
