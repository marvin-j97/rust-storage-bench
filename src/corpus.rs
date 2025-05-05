use rand::{rngs::ThreadRng, Rng, RngCore};
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Eq, PartialEq, clap::ValueEnum, Serialize, Deserialize)]
pub enum Corpus {
    /// Random bytes
    Random,

    /// JSON data
    Json,

    /// Code
    Code,

    /// Natural language
    English,
    // Xml,
}

impl Corpus {
    pub fn fetch(&self, rng: &mut ThreadRng, v: &mut [u8]) {
        match self {
            Self::Random => {
                rng.fill_bytes(v);
            }
            Self::Json => {
                let json_corpus = include_str!("../corpus/asteroids.json");

                let take = v.len();
                let corpus_len = json_corpus.len();
                let max = corpus_len - take;

                let start = rng.gen_range(0..=max);

                v.copy_from_slice(json_corpus.get(start..start + take).unwrap().as_bytes());
            }
            Self::Code => {
                let code_corpus = include_str!("../corpus/rust.rs");

                let take = v.len();
                let corpus_len = code_corpus.len();
                let max = corpus_len - take;

                let start = rng.gen_range(0..=max);

                v.copy_from_slice(code_corpus.get(start..start + take).unwrap().as_bytes());
            }
            Self::English => {
                let eng_corpus = include_str!("../corpus/english.txt");

                let take = v.len();
                let corpus_len = eng_corpus.len();
                let max = corpus_len - take;

                let start = rng.gen_range(0..=max);

                v.copy_from_slice(eng_corpus.get(start..start + take).unwrap().as_bytes());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn corpus_random() {
        let mut buf = vec![0; 64];
        let mut rng = rand::thread_rng();

        assert_eq!(&[0; 64], &*buf);

        Corpus::Random.fetch(&mut rng, &mut buf);

        assert!([0; 64] != *buf);
        let after = buf.to_vec();

        Corpus::Random.fetch(&mut rng, &mut buf);

        assert!([0; 64] != *buf);
        assert!(after != *buf);
    }

    #[test]
    fn corpus_json() {
        let mut buf = vec![0; 64];
        let mut rng = rand::thread_rng();

        assert_eq!(&[0; 64], &*buf);

        Corpus::Json.fetch(&mut rng, &mut buf);

        assert!([0; 64] != *buf);
        let after = buf.to_vec();

        Corpus::Json.fetch(&mut rng, &mut buf);

        assert!([0; 64] != *buf);
        assert!(after != *buf);
    }
}
