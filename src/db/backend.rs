use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Copy, Eq, PartialEq, Debug, Clone, ValueEnum, Serialize, Deserialize)]
#[clap(rename_all = "kebab_case")]
pub enum Backend {
    #[serde(rename = "fjall")]
    Fjall,

    #[serde(rename = "sled")]
    Sled,

    #[serde(rename = "redb")]
    Redb,
    /*     #[serde(rename = "bloodstone")]
    Bloodstone, */
    // Persy,
    // JammDb,
    // Redb,
    // Nebari,

    // #[cfg(feature = "heed")]
    // Heed,

    // #[cfg(feature = "rocksdb")]
    // RocksDb,
}

impl std::fmt::Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Sled => "sled 0.34.7",
                Self::Fjall => "fjall 2.0.3",
                Self::Redb => "redb 2.1.3",
                // Self::Bloodstone => "sled 1.0.0-alpha.122",
                // Self::Persy => "persy 1.5.0",
                // Self::JammDb => "jammdb 0.11.0",
                // Self::Nebari => "nebari 0.5.5",

                // #[cfg(feature = "heed")]
                // Self::Heed => "heed 0.20.0",

                // #[cfg(feature = "rocksdb")]
                // Self::RocksDb => "rocksdb 0.22.0",
            }
        )
    }
}
