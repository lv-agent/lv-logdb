//! gRPC client for logdbd: connect, failover, Scan, Tail, GetWatermark.
//! Supports TLS and mTLS via config.

use logdbd_proto::pb::log_db_service_client::LogDbServiceClient;
use logdbd_proto::pb::{
    GetWatermarkRequest, ScanRequest, ScanResponse, TailRequest, TailResponse, Watermark,
};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::config;

pub struct Source {
    client: LogDbServiceClient<Channel>,
    addrs: Vec<String>,
    current_idx: usize,
    tls_config: config::TlsConfig,
}

impl Source {
    pub async fn connect(addrs: &[String], tls_config: config::TlsConfig) -> Result<Self, String> {
        for (i, addr) in addrs.iter().enumerate() {
            match connect_addr(addr, &tls_config).await {
                Ok(client) => {
                    return Ok(Self {
                        client,
                        addrs: addrs.to_vec(),
                        current_idx: i,
                        tls_config,
                    });
                }
                Err(e) => {
                    tracing::warn!(addr = %addr, error = %e, "failed to connect, trying next");
                }
            }
        }
        Err("all source addresses failed".into())
    }

    pub async fn failover(&mut self) -> Result<(), String> {
        self.current_idx = (self.current_idx + 1) % self.addrs.len();
        let addr = &self.addrs[self.current_idx];
        self.client = connect_addr(addr, &self.tls_config)
            .await
            .map_err(|e| format!("failover to {}: {}", addr, e))?;
        Ok(())
    }

    pub async fn get_watermark(&mut self, ns: &str, stream: &str) -> Result<Watermark, String> {
        let resp = self
            .client
            .get_watermark(GetWatermarkRequest {
                namespace: ns.into(),
                stream: stream.into(),
            })
            .await
            .map_err(|e| format!("get_watermark: {}", e))?;
        Ok(resp.into_inner())
    }

    pub async fn scan(
        &mut self,
        ns: &str,
        stream: &str,
        from: u64,
        limit: u32,
    ) -> Result<Vec<ScanResponse>, String> {
        let resp = self
            .client
            .scan(ScanRequest {
                namespace: ns.into(),
                stream: stream.into(),
                from_seq: from,
                to_seq: 0,
                limit,
            })
            .await
            .map_err(|e| format!("scan: {}", e))?;
        let mut stream = resp.into_inner();
        let mut results = Vec::new();
        while let Some(chunk) = stream
            .message()
            .await
            .map_err(|e| format!("scan stream: {}", e))?
        {
            results.push(chunk);
        }
        Ok(results)
    }

    pub async fn tail(
        &mut self,
        ns: &str,
        stream: &str,
        from: u64,
        batch_size: u32,
    ) -> Result<tonic::Streaming<TailResponse>, String> {
        let resp = self
            .client
            .tail(TailRequest {
                namespace: ns.into(),
                stream: stream.into(),
                from_seq: from,
                batch_size,
                ..Default::default()
            })
            .await
            .map_err(|e| format!("tail: {}", e))?;
        Ok(resp.into_inner())
    }
}

async fn connect_addr(
    addr: &str,
    tls_config: &config::TlsConfig,
) -> Result<LogDbServiceClient<Channel>, String> {
    match tls_config.mode {
        config::TlsMode::Disabled => {
            let uri: tonic::transport::Uri = format!("http://{}", addr)
                .parse()
                .map_err(|e| format!("invalid URI {}: {}", addr, e))?;
            let channel = Channel::builder(uri).connect_lazy();
            Ok(LogDbServiceClient::new(channel))
        }
        config::TlsMode::Tls => {
            let tls = build_client_tls(tls_config)?;
            let uri: tonic::transport::Uri = format!("https://{}", addr)
                .parse()
                .map_err(|e| format!("invalid URI {}: {}", addr, e))?;
            let channel = Endpoint::from(uri)
                .tls_config(tls)
                .map_err(|e| format!("TLS config: {}", e))?
                .connect_lazy();
            Ok(LogDbServiceClient::new(channel))
        }
        config::TlsMode::Mtls => {
            let tls = build_client_mtls(tls_config)?;
            let uri: tonic::transport::Uri = format!("https://{}", addr)
                .parse()
                .map_err(|e| format!("invalid URI {}: {}", addr, e))?;
            let channel = Endpoint::from(uri)
                .tls_config(tls)
                .map_err(|e| format!("mTLS config: {}", e))?
                .connect_lazy();
            Ok(LogDbServiceClient::new(channel))
        }
    }
}

fn build_client_tls(tls_config: &config::TlsConfig) -> Result<ClientTlsConfig, String> {
    let mut tls = ClientTlsConfig::new();
    if let Some(ref ca) = tls_config.ca_file {
        let ca_pem = std::fs::read(ca).map_err(|e| format!("read CA {}: {}", ca, e))?;
        tls = tls.ca_certificate(Certificate::from_pem(ca_pem));
    }
    Ok(tls)
}

fn build_client_mtls(tls_config: &config::TlsConfig) -> Result<ClientTlsConfig, String> {
    let mut tls = build_client_tls(tls_config)?;
    match (tls_config.cert_file.as_ref(), tls_config.key_file.as_ref()) {
        (Some(cert), Some(key)) => {
            let cert_pem = std::fs::read(cert).map_err(|e| format!("read cert {}: {}", cert, e))?;
            let key_pem = std::fs::read(key).map_err(|e| format!("read key {}: {}", key, e))?;
            let identity = Identity::from_pem(cert_pem, key_pem);
            tls = tls.identity(identity);
        }
        _ => return Err("mTLS requires cert_file and key_file".into()),
    }
    Ok(tls)
}
