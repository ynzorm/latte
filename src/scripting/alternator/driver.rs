pub use aws_sdk_dynamodb::*;

#[cfg(feature = "alternator-new")]
pub use alternator_driver::*;

/// DynamoDB client used for Alternator (plain AWS SDK vs alternator-driver).
#[cfg(feature = "alternator-new")]
pub type Client = AlternatorClient;
#[cfg(not(feature = "alternator-new"))]
pub type Client = aws_sdk_dynamodb::Client;

/// Builds the appropriate DynamoDB client for the enabled Alternator backend.
pub fn new_client(
    sdk_config: &aws_config::SdkConfig,
    conf: &crate::config::ConnectionConf,
) -> Client {
    #[cfg(feature = "alternator-new")]
    {
        new_alternator_client(sdk_config, &conf.alternator_new)
    }
    #[cfg(not(feature = "alternator-new"))]
    {
        let _ = conf;
        aws_sdk_dynamodb::Client::new(sdk_config)
    }
}

#[cfg(feature = "alternator-new")]
fn new_alternator_client(
    sdk_config: &aws_config::SdkConfig,
    opts: &super::config::AlternatorNewConnectionOpts,
) -> AlternatorClient {
    use super::config::AlternatorRequestCompressionMode;

    let mut builder = AlternatorConfig::new(sdk_config).to_builder();
    if let Some(optimize) = opts.optimize_headers {
        builder = builder.optimize_headers(optimize);
    }
    match opts.request_compression {
        AlternatorRequestCompressionMode::DriverDefault => {}
        AlternatorRequestCompressionMode::Off => {
            builder = builder.request_compression(RequestCompression::disabled());
        }
        AlternatorRequestCompressionMode::Gzip => {
            let level = opts
                .compression_level
                .map(|n| CompressionLevel::new(n as u32))
                .unwrap_or_default();
            builder = builder.request_compression(RequestCompression::enabled(
                CompressionAlgorithm::Gzip,
                level,
                opts.compression_threshold,
            ));
        }
        AlternatorRequestCompressionMode::Zlib => {
            let level = opts
                .compression_level
                .map(|n| CompressionLevel::new(n as u32))
                .unwrap_or_default();
            builder = builder.request_compression(RequestCompression::enabled(
                CompressionAlgorithm::Zlib,
                level,
                opts.compression_threshold,
            ));
        }
    }
    AlternatorClient::from_conf(builder.build())
}
