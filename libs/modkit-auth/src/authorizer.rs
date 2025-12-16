use crate::{claims::Claims, errors::AuthError, traits::PrimaryAuthorizer, types::SecRequirement};
use async_trait::async_trait;

/// Role-based authorizer that checks resource:action patterns
#[derive(Debug, Clone, Default)]
pub struct RoleAuthorizer;

impl RoleAuthorizer {
    /// Check if any role matches the requirement pattern
    fn check_role(claims: &Claims, requirement: &SecRequirement) -> bool {
        fn role_matches(role_pattern: &str, req_resource: &str, req_action: &str) -> bool {
            match role_pattern {
                "*:*" => true,
                pattern if pattern.ends_with(":*") => {
                    let resource = &pattern[..pattern.len() - 2];
                    resource == req_resource
                }
                pattern if pattern.starts_with("*:") => {
                    let action = &pattern[2..];
                    req_action == action
                }
                _ => role_pattern == format!("{req_resource}:{req_action}"),
            }
        }

        requirement
            .actions
            .iter()
            .map(|action| (requirement.resource.as_str(), action.as_str()))
            .any(|required_role| {
                claims
                    .roles
                    .iter()
                    .any(|role| role_matches(role, required_role.0, required_role.1))
            })
    }
}

#[async_trait]
impl PrimaryAuthorizer for RoleAuthorizer {
    async fn check(&self, claims: &Claims, requirement: &SecRequirement) -> Result<(), AuthError> {
        if Self::check_role(claims, requirement) {
            Ok(())
        } else {
            Err(AuthError::Forbidden)
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn mock_claims(roles: Vec<String>) -> Claims {
        Claims {
            sub: Uuid::new_v4(),
            issuer: "test".to_owned(),
            audiences: vec![],
            expires_at: None,
            not_before: None,
            tenants: vec![],
            roles,
            extras: serde_json::Map::new(),
        }
    }

    #[tokio::test]
    async fn test_exact_role_match() {
        let auth = RoleAuthorizer;
        let claims = mock_claims(vec!["users:read".to_owned()]);
        let req = SecRequirement::new("users",["read"]);

        assert!(auth.check(&claims, &req).await.is_ok());
    }

    #[tokio::test]
    async fn test_resource_wildcard() {
        let auth = RoleAuthorizer;
        let claims = mock_claims(vec!["users:*".to_owned()]);
        let req = SecRequirement::new("users", ["write"]);

        assert!(auth.check(&claims, &req).await.is_ok());
    }

    #[tokio::test]
    async fn test_action_wildcard() {
        let auth = RoleAuthorizer;
        let claims = mock_claims(vec!["*:read".to_owned()]);
        let req = SecRequirement::new("posts", ["read"]);

        assert!(auth.check(&claims, &req).await.is_ok());
    }

    #[tokio::test]
    async fn test_full_wildcard() {
        let auth = RoleAuthorizer;
        let claims = mock_claims(vec!["*:*".to_owned()]);
        let req = SecRequirement::new("anything", ["everything"]);

        assert!(auth.check(&claims, &req).await.is_ok());
    }

    #[tokio::test]
    async fn test_no_matching_role() {
        let auth = RoleAuthorizer;
        let claims = mock_claims(vec!["posts:read".to_owned()]);
        let req = SecRequirement::new("users", ["read"]);

        assert!(matches!(
            auth.check(&claims, &req).await,
            Err(AuthError::Forbidden)
        ));
    }
}
