// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Pull-based bearer access-token projection for SDK adapters.

use super::UnsupportedProjection;
use crate::capability::CapabilityError;
use crate::capability::auth::BearerToken;
use crate::capability::auth::client_credential_provider::{
    CredentialMaterial, CredentialProjection,
};
use crate::shared::capability::auth::client_credential_provider::ClientCredentialProvider;
use std::time::Instant;
use thiserror::Error;

/// A bearer access token returned through the approved token projection.
#[derive(Clone, Debug)]
pub struct BearerAccessToken {
    token: BearerToken,
    expires_on: Option<Instant>,
}

impl BearerAccessToken {
    /// Explicitly exposes the projected token for an SDK adapter.
    #[must_use]
    pub fn expose_token(&self) -> &str {
        self.token.expose_token()
    }

    /// Returns the token expiry, when known.
    #[must_use]
    pub const fn expires_on(&self) -> Option<Instant> {
        self.expires_on
    }
}

/// A bearer access token could not be projected.
#[derive(Debug, Error)]
pub enum ProjectionError {
    /// The provider failed to return its current credential.
    #[error(transparent)]
    Provider(#[from] CapabilityError),
    /// The provider returned material inconsistent with its binding declaration.
    #[error("provider declared bearer access-token support but returned non-bearer material")]
    InconsistentProvider,
}

/// Pull-based bearer token source used by an SDK adapter.
pub struct BearerAccessTokenSource {
    provider: Box<dyn ClientCredentialProvider>,
}

impl BearerAccessTokenSource {
    /// Pulls the provider's current bearer token.
    pub async fn current(&self) -> Result<BearerAccessToken, ProjectionError> {
        let snapshot = self.provider.current().await?;
        let CredentialMaterial::Bearer(secret) = &snapshot.credential.material else {
            return Err(ProjectionError::InconsistentProvider);
        };
        Ok(BearerAccessToken {
            token: secret.clone(),
            expires_on: snapshot.expires_on(),
        })
    }
}

/// Binds a shared credential provider to a pull-based bearer token source.
pub fn bind(
    provider: Box<dyn ClientCredentialProvider>,
) -> Result<BearerAccessTokenSource, UnsupportedProjection> {
    let projection = CredentialProjection::BearerAccessToken;
    if !provider.supports(projection) {
        return Err(UnsupportedProjection { projection });
    }
    Ok(BearerAccessTokenSource { provider })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::auth::client_credential_provider::{
        ClientCredential, CredentialSnapshot, CredentialStream,
    };
    use async_trait::async_trait;
    use futures::stream;

    struct TestProvider {
        snapshot: CredentialSnapshot,
        supports_token: bool,
    }

    #[async_trait]
    impl ClientCredentialProvider for TestProvider {
        fn supports(&self, projection: CredentialProjection) -> bool {
            self.supports_token && projection == CredentialProjection::BearerAccessToken
        }

        async fn current(&self) -> Result<CredentialSnapshot, CapabilityError> {
            Ok(self.snapshot.clone())
        }

        fn credential_stream(&self) -> CredentialStream {
            Box::pin(stream::once(std::future::ready(self.snapshot.clone())))
        }
    }

    /// Scenario: bind a bearer provider to the access-token projection.
    /// Guarantees: each pull returns the current token, generation, and expiry.
    #[tokio::test]
    async fn bearer_provider_projects_current_access_token() {
        let expires_on = Some(Instant::now());
        let provider = Box::new(TestProvider {
            snapshot: CredentialSnapshot::new(
                ClientCredential::bearer("token".to_owned()),
                expires_on,
            ),
            supports_token: true,
        });

        let source = bind(provider).expect("Bearer supports token projection");
        let token = source.current().await.expect("current token");

        assert_eq!(token.expose_token(), "token");
        assert_eq!(token.expires_on(), expires_on);
    }

    /// Scenario: bind a Basic provider to the bearer access-token projection.
    /// Guarantees: the unsupported use is rejected before the node processes data.
    #[test]
    fn basic_provider_is_rejected_during_binding() {
        let provider = Box::new(TestProvider {
            snapshot: CredentialSnapshot::new(
                ClientCredential::basic("user".to_owned(), "password".to_owned()),
                None,
            ),
            supports_token: false,
        });

        assert!(matches!(
            bind(provider),
            Err(UnsupportedProjection {
                projection: CredentialProjection::BearerAccessToken,
            })
        ));
    }
}
