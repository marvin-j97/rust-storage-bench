use rand::{Rng, RngCore};
use serde::{Deserialize, Serialize};

const JSON_CORPUS: &[u8] = include_str!("../corpus/asteroids.json").as_bytes();
const CODE_CORPUS: &[u8] = include_str!("../corpus/rust.rs").as_bytes();
const ENGLISH_CORPUS: &[u8] = include_str!("../corpus/english.txt").as_bytes();
const HTML_CORPUS: &[u8] = include_str!("../corpus/webpage.html").as_bytes();

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

    /// Web page
    Html,
}

impl Corpus {
    pub fn fetch(&self, rng: &mut impl RngCore, mut v: &mut [u8]) {
        let corpus = match self {
            Self::Random => {
                rng.fill_bytes(v);
                return;
            }
            Self::Json => JSON_CORPUS,
            Self::Code => CODE_CORPUS,
            Self::English => ENGLISH_CORPUS,
            Self::Html => HTML_CORPUS,
        };

        while !v.is_empty() {
            let take = v.len().min(corpus.len());

            let start = if corpus.len() == take {
                0
            } else {
                rng.gen_range(0..corpus.len() - take)
            };

            v[..take].copy_from_slice(&corpus[start..start + take]);
            v = &mut v[take..];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn corpus_random() {
        let mut buf = vec![0; 64];
        let mut rng = crate::random::thread_rng();

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
        let mut rng = crate::random::thread_rng();

        assert_eq!(&[0; 64], &*buf);

        Corpus::Json.fetch(&mut rng, &mut buf);

        assert!([0; 64] != *buf);
        let after = buf.to_vec();

        Corpus::Json.fetch(&mut rng, &mut buf);

        assert!([0; 64] != *buf);
        assert!(after != *buf);
    }

    #[test]
    fn larger_than_corpus() {
        const BUF_LEN: usize = 1024 * 1024;
        assert!(CODE_CORPUS.len() < BUF_LEN);

        let mut buf = vec![0; BUF_LEN];
        let mut rng = crate::random::thread_rng();

        assert_eq!(&[0; BUF_LEN], &*buf);

        Corpus::Code.fetch(&mut rng, &mut buf);
    }
}
