//! Authentication for the gRPC service.
//!
//! P0-3: logdbd must not accept unauthenticated traffic. When an auth token is
//! configured (`LOGDBD_AUTH_TOKEN`), every RPC to `LogDbService` and
//! `ReplicationService` must carry `authorization: Bearer <token>` or be
//! rejected with `UNAUTHENTICATED`. The health service is left open so standard
//! gRPC health probes keep working.

use std::sync::Arc;

use tonic::service::Interceptor;
use tonic::{Request, Status};

/// The metadata header we check. Standard "Authorization: Bearer <token>".
const HEADER: &str = "authorization";

/// A tonic interceptor that validates a bearer token.
#[derive(Clone)]
pub struct AuthInterceptor {
    /// The exact value expected in the `authorization` header
    /// (`"Bearer <token>"`). In an `Arc` so the cloned-per-RPC interceptor is
    /// cheap to clone.
    expected: Arc<String>,
}

impl AuthInterceptor {
    /// Build an interceptor expecting `Bearer <token>`.
    pub fn new(token: &str) -> Self {
        Self {
            expected: Arc::new(format!("Bearer {}", token)),
        }
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        let ok = req
            .metadata()
            .get(HEADER)
            .and_then(|v| v.to_str().ok())
            .map(|got| got == self.expected.as_str())
            .unwrap_or(false);
        if ok {
            Ok(req)
        } else {
            Err(Status::unauthenticated("missing or invalid auth token"))
        }
    }
}

/// Build the `authorization` header value a client should send for `token`.
pub fn bearer_header(token: &str) -> String {
    format!("Bearer {}", token)
}
