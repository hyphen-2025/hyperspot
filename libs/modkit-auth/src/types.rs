/// Security requirement - defines required resource and action
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecRequirement {
    pub resource: String,
    pub actions: Vec<String>,
}

impl SecRequirement {
    pub fn new<S>(resource: impl Into<String>, actions: impl IntoIterator<Item=S>) -> Self 
    where S: Into<String> {
        Self {
            resource: resource.into(),
            actions: actions.into_iter().map(Into::into).collect(),
        }
    }
}

/// Route-level authentication requirement
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthRequirement {
    /// No authentication required; route is public from auth perspective.
    None,
    /// Authentication required; `None` means no extra RBAC requirement,
    /// `Some(SecRequirement)` means enforce this resource:action requirement.
    Required(Option<SecRequirement>),
    /// Optional authentication: if a valid token is present, use it;
    /// otherwise proceed anonymously.
    Optional,
}

#[async_trait::async_trait]
impl RoutePolicy for AuthRequirement {
    async fn resolve(&self, _method: &http::Method, _path: &str) -> AuthRequirement {
        self.clone()
    }
}

/// Route policy that determines authentication requirements for routes
#[async_trait::async_trait]
pub trait RoutePolicy: Send + Sync {
    /// Resolve the authentication requirement for a given method and path
    async fn resolve(&self, method: &http::Method, path: &str) -> AuthRequirement;
}
