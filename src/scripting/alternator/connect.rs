use super::alternator_error::{AlternatorError, AlternatorErrorKind};
use super::context::Context;
use super::driver::config::{Credentials, Region};
use super::driver::error::DisplayErrorContext;
#[cfg(feature = "alternator-new")]
use super::driver::AlternatorClient as Client;
#[cfg(not(feature = "alternator-new"))]
use super::driver::Client;
use crate::config::ConnectionConf;
use aws_config::retry::RetryConfig;
use aws_config::BehaviorVersion;

pub async fn connect(conf: &ConnectionConf) -> Result<Context, AlternatorError> {
    let address = conf.addresses.first().cloned().unwrap_or_default();

    // TODO: use latte parameters for setting the configuration
    let config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(&address)
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("", "", None, None, ""))
        .retry_config(RetryConfig::standard().with_max_attempts(1))
        .load()
        .await;

    let client = Client::new(&config);

    // Validate connection by making a test request
    client.list_tables().limit(1).send().await.map_err(|e| {
        AlternatorError(AlternatorErrorKind::FailedToConnect(
            address,
            DisplayErrorContext(&e).to_string(),
        ))
    })?;

    Ok(Context::new(
        Some(client),
        conf.retry_number,
        conf.retry_interval,
        conf.validation_strategy,
        conf.page_size.get() as u64,
    ))
}
