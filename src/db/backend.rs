use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Copy, Eq, PartialEq, Debug, Clone, ValueEnum, Serialize, Deserialize)]
#[clap(rename_all = "kebab_case")]
pub enum Backend {
    #[serde(rename = "fjall_2")]
    Fjall2,

    #[cfg(feature = "fjall_3")]
    #[serde(rename = "fjall_3")]
    #[serde(alias = "fjall_3")]
    Fjall3,

    #[serde(rename = "sled")]
    Sled,

    #[serde(rename = "redb")]
    Redb,

    #[cfg(feature = "heed")]
    #[serde(rename = "heed")]
    Heed,

    #[cfg(feature = "rocksdb")]
    #[serde(rename = "rocksdb")]
    #[serde(alias = "rocks")]
    #[clap(name = "rocksdb", alias = "rocks")]
    RocksDb,

    #[cfg(feature = "sqlite")]
    #[serde(rename = "sqlite")]
    Sqlite,

    #[serde(rename = "canopydb")]
    Canopydb,
}

impl std::fmt::Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                #[cfg(feature = "sqlite")]
                Self::Sqlite => "rusqlite 0.32.1",

                Self::Sled => "sled 0.34.7",

                Self::Fjall2 => "fjall 2.11.2",

                #[cfg(feature = "fjall_3")]
                Self::Fjall3 => "fjall 3.0.0-rc.5",

                Self::Redb => "redb 3.1.0",

                #[cfg(feature = "heed")]
                Self::Heed => "heed 0.20.5",

                #[cfg(feature = "rocksdb")]
                Self::RocksDb => "rust_rocksdb 0.44.2",

                Self::Canopydb => "canopydb 0.2.4",
            }
        )
    }
}
