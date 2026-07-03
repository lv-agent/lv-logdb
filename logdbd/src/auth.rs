//! Authentication and RBAC for the gRPC service.
//!
//! Each token is mapped to one or more roles. The interceptor validates
//! the `authorization: Bearer <token>` header and injects an `AuthContext`
//! into the request extensions.  RPC handlers call `require_role` to gate
//! access.
//!
//! Roles:
//! - `admin`:      all operations
//! - `writer`:     append + read
//! - `reader`:     read + query + subscribe
//! - `subscriber`: subscribe only

use std::collections::HashMap;
use std::sync::Arc;

use tonic::service::Interceptor;
use tonic::{Request, Status};

/// Authorisation role.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Role {
    Admin,
    Writer,
    Reader,
    Subscriber,
}

impl Role {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "admin" => Some(Self::Admin),
            "writer" => Some(Self::Writer),
            "reader" => Some(Self::Reader),
            "subscriber" => Some(Self::Subscriber),
            _ => None,
        }
    }
}

/// Injected into every authenticated request.
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub roles: Vec<Role>,
}

impl AuthContext {
    fn has(&self, role: &Role) -> bool {
        self.roles.contains(&Role::Admin) || self.roles.contains(role)
    }
}

/// Token entry from configuration.
#[derive(Debug, Clone)]
pub struct TokenEntry {
    pub token: String,
    pub roles: Vec<Role>,
}

/// gRPC interceptor that validates bearer tokens and injects [`AuthContext`].
#[derive(Clone)]
pub struct AuthInterceptor {
    tokens: Arc<HashMap<String, Vec<Role>>>,
}

impl AuthInterceptor {
    pub fn new(entries: &[TokenEntry]) -> Self {
        let mut tokens = HashMap::new();
        for e in entries {
            tokens.insert(format!("Bearer {}", e.token), e.roles.clone());
        }
        Self {
            tokens: Arc::new(tokens),
        }
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        let got = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        match self.tokens.get(got) {
            Some(roles) => {
                req.extensions_mut().insert(AuthContext {
                    roles: roles.clone(),
                });
                Ok(req)
            }
            None => Err(Status::unauthenticated("missing or invalid auth token")),
        }
    }
}

/// A no-op interceptor that injects admin context when auth is disabled.
#[derive(Clone)]
pub struct NoAuthInterceptor;

impl Interceptor for NoAuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        req.extensions_mut().insert(AuthContext {
            roles: vec![Role::Admin],
        });
        Ok(req)
    }
}

/// Unified interceptor enum — tonic needs Concrete + Clone.
#[derive(Clone)]
pub enum AnyInterceptor {
    Auth(AuthInterceptor),
    NoAuth(NoAuthInterceptor),
}

impl Interceptor for AnyInterceptor {
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        match self {
            Self::Auth(a) => a.call(req),
            Self::NoAuth(n) => n.call(req),
        }
    }
}

/// Gate an RPC handler — returns `PERMISSION_DENIED` if the context
/// doesn't have the required role.
pub fn require_role<T>(req: &Request<T>, role: Role) -> Result<(), Status> {
    match req.extensions().get::<AuthContext>() {
        None => Ok(()), // auth not configured — allow all
        Some(ctx) => {
            if ctx.has(&role) {
                Ok(())
            } else {
                Err(Status::permission_denied(format!(
                    "requires {:?} role",
                    role
                )))
            }
        }
    }
}
