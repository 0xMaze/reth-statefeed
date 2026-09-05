//! Configuration loading and validation.

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use alloy_primitives::{Address, B256};
use serde::Deserialize;
use thiserror::Error;

/// Complete statefeed configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Local stream settings.
    #[serde(default)]
    pub stream: StreamConfig,
    /// Physical Ethereum storage keys to project.
    pub watch: Vec<WatchConfig>,
}

/// Unix socket and bounded-buffer settings.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct StreamConfig {
    /// Publish speculative state immediately after EVM execution, before full validation.
    pub publish_executed: bool,
    /// Unix socket exposed to local consumers.
    pub socket: PathBuf,
    /// Unix permission bits applied to the socket path.
    pub socket_mode: u32,
    /// Number of block/control events buffered between validation and the publisher.
    pub queue_capacity: usize,
    /// Number of candidate projections retained for forks and reorgs.
    pub candidate_cache_blocks: usize,
    /// Number of lightweight candidate ancestry records retained after projection eviction.
    pub candidate_metadata_entries: usize,
    /// Maximum local lifetime of a non-canonical candidate without a fresh observation.
    #[serde(with = "humantime_serde")]
    pub candidate_retention: Duration,
    /// Maximum number of metadata records retired by one maintenance pass.
    pub retirement_work_budget: usize,
    /// Broadcast frames retained in the shared ring for lagging consumers.
    pub consumer_buffer: usize,
    /// Maximum number of concurrently connected local consumers.
    pub max_consumers: usize,
    /// Maximum encoded protobuf payload accepted by the framing layer.
    pub max_frame_bytes: usize,
    /// Optional logical CPU on which the synchronous publisher thread is pinned.
    pub publisher_cpu: Option<usize>,
    /// Busy-spin window after each empty queue poll before the publisher parks.
    pub publisher_spin_us: u64,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            publish_executed: false,
            socket: PathBuf::from("/run/reth-statefeed/statefeed.sock"),
            socket_mode: 0o660,
            queue_capacity: 8_192,
            candidate_cache_blocks: 128,
            candidate_metadata_entries: 1_024,
            candidate_retention: Duration::from_secs(120),
            retirement_work_budget: 256,
            consumer_buffer: 256,
            max_consumers: 64,
            max_frame_bytes: 4 * 1024 * 1024,
            publisher_cpu: None,
            publisher_spin_us: 0,
        }
    }
}

/// One named physical storage coordinate.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WatchConfig {
    /// Stable, consumer-facing identity of the value.
    pub id: String,
    /// Account whose storage is read.
    pub address: Address,
    /// Final 32-byte storage key, including any mapping-key hashing.
    pub slot: B256,
}

/// Configuration error reported before the node is launched.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The file could not be read.
    #[error("failed to read statefeed config {path}: {source}")]
    Read {
        /// Config path.
        path: PathBuf,
        /// Filesystem error.
        source: std::io::Error,
    },
    /// TOML decoding failed.
    #[error("failed to parse statefeed config {path}: {source}")]
    Parse {
        /// Config path.
        path: PathBuf,
        /// TOML error.
        source: toml::de::Error,
    },
    /// A semantic invariant is not satisfied.
    #[error("invalid statefeed config: {0}")]
    Invalid(String),
}

impl Config {
    /// Reads, decodes, and semantically validates a TOML configuration.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        let config: Self = toml::from_str(&contents).map_err(|source| ConfigError::Parse {
            path: path.to_owned(),
            source,
        })?;
        config.validate()?;
        Ok(config)
    }

    /// Validates invariants relied upon by fixed-width projections and bounded queues.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.watch.is_empty() {
            return Err(ConfigError::Invalid(
                "watch must contain at least one key".into(),
            ));
        }
        if self.watch.len() > u32::MAX as usize {
            return Err(ConfigError::Invalid(
                "watch contains more than u32::MAX keys".into(),
            ));
        }
        self.stream.validate()?;

        let mut ids = HashSet::with_capacity(self.watch.len());
        let mut coordinates = HashSet::with_capacity(self.watch.len());
        for key in &self.watch {
            if key.id.trim().is_empty() {
                return Err(ConfigError::Invalid("watch id must not be empty".into()));
            }
            if !ids.insert(key.id.as_str()) {
                return Err(ConfigError::Invalid(format!(
                    "duplicate watch id: {}",
                    key.id
                )));
            }
            if !coordinates.insert((key.address, key.slot)) {
                return Err(ConfigError::Invalid(format!(
                    "duplicate storage coordinate: {} / {}",
                    key.address, key.slot
                )));
            }
        }

        let projection_bytes = self
            .watch
            .len()
            .saturating_mul(32)
            .saturating_add(self.watch.len().div_ceil(8))
            .saturating_add(512);
        let dictionary_bytes = self.watch.iter().fold(512usize, |size, key| {
            size.saturating_add(80).saturating_add(key.id.len())
        });
        let required_frame_bytes = projection_bytes.max(dictionary_bytes);
        if required_frame_bytes > self.stream.max_frame_bytes {
            return Err(ConfigError::Invalid(format!(
                "watch set requires approximately {required_frame_bytes} payload bytes, exceeding stream.max_frame_bytes"
            )));
        }
        Ok(())
    }
}

impl StreamConfig {
    /// Validates invariants required by the public service API before it creates resources.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.queue_capacity < 2 {
            return Err(ConfigError::Invalid(
                "stream.queue_capacity must be at least 2".into(),
            ));
        }
        if self.consumer_buffer == 0 {
            return Err(ConfigError::Invalid(
                "stream.consumer_buffer must be positive".into(),
            ));
        }
        if self.max_consumers == 0 {
            return Err(ConfigError::Invalid(
                "stream.max_consumers must be positive".into(),
            ));
        }
        if self.candidate_cache_blocks < 2 {
            return Err(ConfigError::Invalid(
                "stream.candidate_cache_blocks must be at least 2".into(),
            ));
        }
        if self.candidate_metadata_entries < self.candidate_cache_blocks {
            return Err(ConfigError::Invalid(
                "stream.candidate_metadata_entries must be at least stream.candidate_cache_blocks"
                    .into(),
            ));
        }
        if self.candidate_retention.is_zero() {
            return Err(ConfigError::Invalid(
                "stream.candidate_retention must be positive".into(),
            ));
        }
        if self.retirement_work_budget == 0 {
            return Err(ConfigError::Invalid(
                "stream.retirement_work_budget must be positive".into(),
            ));
        }
        if self.max_frame_bytes < 1024 {
            return Err(ConfigError::Invalid(
                "stream.max_frame_bytes must be at least 1024".into(),
            ));
        }
        if self.publisher_spin_us > 1_000_000 {
            return Err(ConfigError::Invalid(
                "stream.publisher_spin_us must not exceed 1000000".into(),
            ));
        }
        if !self.socket.is_absolute() {
            return Err(ConfigError::Invalid(
                "stream.socket must be an absolute path".into(),
            ));
        }
        if self.socket_mode > 0o777 {
            return Err(ConfigError::Invalid(
                "stream.socket_mode must contain only Unix rwx permission bits".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Config {
        Config {
            stream: StreamConfig {
                socket: PathBuf::from("/tmp/statefeed.sock"),
                ..Default::default()
            },
            watch: vec![WatchConfig {
                id: "value".into(),
                address: Address::ZERO,
                slot: B256::ZERO,
            }],
        }
    }

    #[test]
    fn accepts_minimal_valid_config() {
        let config = config();
        config.validate().unwrap();
        assert!(!config.stream.publish_executed);
    }

    #[test]
    fn rejects_duplicate_coordinates() {
        let mut config = config();
        config.watch.push(WatchConfig {
            id: "other".into(),
            address: Address::ZERO,
            slot: B256::ZERO,
        });
        assert!(matches!(config.validate(), Err(ConfigError::Invalid(_))));
    }

    #[test]
    fn public_stream_validation_rejects_unsafe_zero_bounds() {
        let stream = StreamConfig {
            consumer_buffer: 0,
            ..Default::default()
        };
        assert!(matches!(stream.validate(), Err(ConfigError::Invalid(_))));

        let stream = StreamConfig {
            candidate_cache_blocks: 0,
            ..Default::default()
        };
        assert!(matches!(stream.validate(), Err(ConfigError::Invalid(_))));

        let stream = StreamConfig {
            max_consumers: 0,
            ..Default::default()
        };
        assert!(matches!(stream.validate(), Err(ConfigError::Invalid(_))));

        let stream = StreamConfig {
            candidate_cache_blocks: 16,
            candidate_metadata_entries: 8,
            ..Default::default()
        };
        assert!(matches!(stream.validate(), Err(ConfigError::Invalid(_))));

        let stream = StreamConfig {
            candidate_retention: Duration::ZERO,
            ..Default::default()
        };
        assert!(matches!(stream.validate(), Err(ConfigError::Invalid(_))));

        let stream = StreamConfig {
            retirement_work_budget: 0,
            ..Default::default()
        };
        assert!(matches!(stream.validate(), Err(ConfigError::Invalid(_))));
    }

    #[test]
    fn example_config_stays_parseable_and_valid() {
        let config: Config = toml::from_str(include_str!("../config.example.toml")).unwrap();
        config.validate().unwrap();
        assert_eq!(config.stream.socket_mode, 0o660);
    }

    #[test]
    fn ethereum_mainnet_conversion_config_stays_parseable_and_valid() {
        let config: Config =
            toml::from_str(include_str!("../config.ethereum-mainnet-conversions.toml")).unwrap();
        config.validate().unwrap();
        assert!(config.stream.publish_executed);
        assert_eq!(config.watch.len(), 87);
    }
}
