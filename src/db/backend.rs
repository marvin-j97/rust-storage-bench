use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Copy, Eq, PartialEq, Debug, Clone, ValueEnum, Serialize, Deserialize)]
#[clap(rename_all = "kebab_case")]
pub enum Backend {
    #[serde(rename = "fjall")]
    Fjall,

    #[cfg(feature = "localfjall")]
    #[serde(rename = "local_fjall")]
    #[serde(alias = "localfjall")]
    LocalFjall,

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
    Sqlite,

    #[serde(rename = "canopydb")]
    Canopydb,
    //
    /*     #[serde(rename = "bloodstone")]
    Bloodstone, */
    // Persy,
    // JammDb,
    // Nebari,
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

                Self::Fjall => "fjall 2.8.0",

                #[cfg(feature = "localfjall")]
                Self::LocalFjall => "fjall nightly",

                Self::Redb => "redb 2.4.0",

                #[cfg(feature = "heed")]
                Self::Heed => "heed 0.20.5",

                #[cfg(feature = "rocksdb")]
                Self::RocksDb => "rocksdb 0.22.0",

                Self::Canopydb => "canopydb 0.2.4",
                // Self::Bloodstone => "sled 1.0.0-alpha.122",
                // Self::Persy => "persy 1.5.0",
            }
        )
    }
}
