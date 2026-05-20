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
        new_alternator_client(sdk_config, conf)
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
    conf: &crate::config::ConnectionConf,
) -> AlternatorClient {
    use super::config::{AlternatorKeyRouteAffinity, AlternatorRequestCompressionMode};

    let opts = &conf.alternator_new;
    let mut builder = AlternatorConfig::new(sdk_config).to_builder();

    // Parse seed hosts, scheme, and port from connection addresses
    let mut seed_hosts = Vec::new();
    let mut detected_scheme = None;
    let mut detected_port = None;

    for addr in &conf.addresses {
        let url_str = if addr.contains("://") {
            addr.clone()
        } else {
            format!("http://{}", addr)
        };
        if let Ok(url) = url::Url::parse(&url_str) {
            if let Some(host) = url.host_str() {
                seed_hosts.push(host.to_string());
            }
            if detected_scheme.is_none() {
                detected_scheme = Some(url.scheme().to_string());
            }
            if detected_port.is_none() {
                detected_port = url.port();
            }
        }
    }

    if !seed_hosts.is_empty() {
        builder = builder.seed_hosts(seed_hosts);
    }
    if let Some(scheme) = detected_scheme {
        builder = builder.scheme(scheme);
    }
    if let Some(port) = detected_port {
        builder = builder.port(port);
    }

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

    if let Some(active) = opts.active_interval {
        builder = builder.active_interval(active);
    }
    if let Some(idle) = opts.idle_interval {
        builder = builder.idle_interval(idle);
    }

    let routing_scope = match (&conf.datacenter, &conf.rack) {
        (Some(dc), Some(rack)) => {
            if opts.routing_fallback.unwrap_or(true) {
                RoutingScope::from_rack(dc.clone(), rack.clone())
                    .with_fallback(RoutingScope::from_datacenter(dc.clone()))
                    .with_fallback(RoutingScope::from_cluster())
            } else {
                RoutingScope::from_rack(dc.clone(), rack.clone())
            }
        }
        (Some(dc), None) => {
            if opts.routing_fallback.unwrap_or(true) {
                RoutingScope::from_datacenter(dc.clone())
                    .with_fallback(RoutingScope::from_cluster())
            } else {
                RoutingScope::from_datacenter(dc.clone())
            }
        }
        (None, Some(_)) => {
            panic!("Datacenter must also be defined when rack is defined");
        }
        _ => RoutingScope::from_cluster(),
    };
    builder = builder.routing_scope(routing_scope);

    let affinity_mode =
        opts.key_route_affinity
            .unwrap_or(if !opts.key_route_affinity_tables.is_empty() {
                AlternatorKeyRouteAffinity::Rmw
            } else {
                AlternatorKeyRouteAffinity::None
            });

    if affinity_mode != AlternatorKeyRouteAffinity::None {
        let affinity_type = match affinity_mode {
            AlternatorKeyRouteAffinity::None => {
                keyrouting::affinity_config::KeyRouteAffinityType::None
            }
            AlternatorKeyRouteAffinity::Rmw => {
                keyrouting::affinity_config::KeyRouteAffinityType::Rmw
            }
            AlternatorKeyRouteAffinity::AnyWrite => {
                keyrouting::affinity_config::KeyRouteAffinityType::AnyWrite
            }
        };
        let mut affinity_builder =
            keyrouting::affinity_config::KeyRouteAffinityConfig::builder().with_type(affinity_type);
        for (table, pk) in &opts.key_route_affinity_tables {
            affinity_builder = affinity_builder.with_pk_info(table, pk);
        }
        builder = builder.key_route_affinity(affinity_builder.build());
    }

    AlternatorClient::from_conf(builder.build())
}
