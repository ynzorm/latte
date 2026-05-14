use clap::Parser;
#[cfg(feature = "alternator-new")]
use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug, Default, Serialize, Deserialize)]
pub struct DbConnectionConf {
    /// Use AWS credentials and region from the environment.
    /// Mutually exclusive with `access-key-id`, `secret-access-key` and `region`.
    #[clap(long("aws-credentials"), conflicts_with_all = &["access_key_id", "secret_access_key", "region"])]
    pub aws_credentials: bool,

    /// Access key ID.
    #[clap(long("access-key-id"), default_value = "")]
    pub access_key_id: String,

    /// Secret access key.
    #[serde(skip_serializing)] // Don't save the secret to generated reports.
    #[clap(long("secret-access-key"), default_value = "")]
    pub secret_access_key: String,

    /// Region.
    #[clap(long("region"), default_value = "us-east-1")]
    pub region: String,
}

#[cfg(feature = "alternator-new")]
pub fn parse_compression_level(s: &str) -> Result<u8, String> {
    let n: u8 = s
        .parse()
        .map_err(|_| format!("Invalid compression level: {s} (expected integer 1-9)"))?;
    if (1..=9).contains(&n) {
        Ok(n)
    } else {
        Err("Compression level must be between 1 and 9".to_string())
    }
}

/// HTTP request body compression mode for the alternator-driver client (`latte-alternator-new` only).
#[cfg(feature = "alternator-new")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum AlternatorRequestCompressionMode {
    /// Use alternator-driver defaults.
    #[default]
    DriverDefault,
    Off,
    Gzip,
    Zlib,
}

#[cfg(feature = "alternator-new")]
const DEFAULT_COMPRESSION_THRESHOLD: usize = 1024;

/// Alternator-driver connection options (only when built with `alternator-new`).
#[cfg(feature = "alternator-new")]
#[derive(Parser, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct AlternatorNewConnectionOpts {
    /// How to compress DynamoDB request bodies before signing (alternator-driver).
    #[clap(
        long("request-compression"),
        required = false,
        default_value = "driver-default",
        value_name = "MODE",
        value_enum
    )]
    pub request_compression: AlternatorRequestCompressionMode,

    /// Minimum uncompressed body size in bytes before compression applies (`gzip` / `zlib` only).
    #[clap(
        long("compression-threshold"),
        required = false,
        default_value_t = DEFAULT_COMPRESSION_THRESHOLD,
        value_name = "BYTES"
    )]
    pub compression_threshold: usize,

    /// Deflate compression level 1–9 (`gzip` / `zlib` only). If omitted, the driver default level is used.
    #[clap(
        long("compression-level"),
        required = false,
        value_name = "1-9",
        value_parser = parse_compression_level
    )]
    pub compression_level: Option<u8>,

    /// Strip request headers not used by Alternator before transmit. If omitted, the driver default (true) applies.
    #[clap(long("optimize-headers"), required = false, value_name = "BOOL")]
    pub optimize_headers: Option<bool>,
}

/// Serde hook: missing `compression_threshold` in a report must not deserialize as `usize::default()` (0).
#[cfg(feature = "alternator-new")]
fn default_compression_threshold() -> usize {
    DEFAULT_COMPRESSION_THRESHOLD
}

#[cfg(feature = "alternator-new")]
impl Default for AlternatorNewConnectionOpts {
    fn default() -> Self {
        Self {
            request_compression: AlternatorRequestCompressionMode::default(),
            compression_threshold: default_compression_threshold(),
            compression_level: None,
            optimize_headers: None,
        }
    }
}
