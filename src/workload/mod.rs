use clap::ValueEnum;
use serde::Serialize;

#[derive(Copy, Debug, Clone, ValueEnum, Serialize, PartialEq, Eq)]
#[clap(rename_all = "kebab_case")]
pub enum Workload {
    TimeseriesWrite,
}
