// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Per-consumer `Authorization` header projection.

use super::UnsupportedProjection;
use crate::capability::auth::client_credential_provider::{
    CredentialMaterial, CredentialProjection, CredentialSnapshot, CredentialStream,
};
use crate::local::capability::auth::client_credential_provider::ClientCredentialProvider;
use data_encoding::BASE64;
use futures::StreamExt;
use http::HeaderValue;
use secrecy::ExposeSecret;
use thiserror::Error;

/// One projected authorization header generation.
#[derive(Clone, Debug)]
pub struct AuthorizationHeader {
    value: HeaderValue,
    generation: u64,
}

impl AuthorizationHeader {
    /// Returns the sensitive header value.
    #[must_use]
    pub const fn value(&self) -> &HeaderValue {
        &self.value
    }

    /// Returns the source credential generation.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }
}

/// An authorization header could not be projected.
#[derive(Debug, Error)]
pub enum ProjectionError {
    /// The generated value is not a valid HTTP header.
    #[error("credential could not be encoded as an Authorization header")]
    InvalidHeader(#[from] http::header::InvalidHeaderValue),
}

/// A per-consumer authorization-header view over a credential stream.
pub struct AuthorizationHeaderSource {
    stream: CredentialStream,
    next_generation: u64,
    rejected_generation: Option<u64>,
}

impl AuthorizationHeaderSource {
    /// Returns the next usable projected header.
    ///
    /// A rejected generation is skipped until the provider publishes a newer
    /// generation. Rejection state is local to this source.
    pub async fn next(&mut self) -> Option<Result<AuthorizationHeader, ProjectionError>> {
        while let Some(snapshot) = self.stream.next().await {
            self.next_generation = self.next_generation.wrapping_add(1);
            let generation = self.next_generation;
            if self.rejected_generation == Some(generation) {
                continue;
            }
            self.rejected_generation = None;
            return Some(project(snapshot, generation));
        }
        None
    }

    /// Rejects the generation used by a failed request.
    pub fn reject(&mut self, generation: u64) {
        self.rejected_generation = Some(generation);
    }
}

/// Binds a local credential provider to an authorization-header source.
///
/// This shared projection implementation, not the extension, owns Basic and
/// Bearer formatting and per-consumer rejection state.
pub fn bind(
    provider: Box<dyn ClientCredentialProvider>,
) -> Result<AuthorizationHeaderSource, UnsupportedProjection> {
    let projection = CredentialProjection::AuthorizationHeader;
    if !provider.supports(projection) {
        return Err(UnsupportedProjection { projection });
    }
    Ok(AuthorizationHeaderSource {
        stream: provider.credential_stream(),
        next_generation: 0,
        rejected_generation: None,
    })
}

fn project(
    snapshot: CredentialSnapshot,
    generation: u64,
) -> Result<AuthorizationHeader, ProjectionError> {
    let value = match &snapshot.credential.material {
        CredentialMaterial::Bearer(secret) => {
            HeaderValue::from_str(&format!("Bearer {}", secret.expose_token()))?
        }
        CredentialMaterial::Basic { username, password } => {
            let plaintext = format!("{}:{}", username.expose_secret(), password.expose_secret());
            HeaderValue::from_str(&format!("Basic {}", BASE64.encode(plaintext.as_bytes())))?
        }
    };
    let mut value = value;
    value.set_sensitive(true);
    Ok(AuthorizationHeader { value, generation })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::CapabilityError;
    use crate::capability::auth::client_credential_provider::{
        ClientCredential, CredentialSnapshot,
    };
    use async_trait::async_trait;
    use futures::stream;
    use std::collections::VecDeque;

    struct TestProvider {
        current: CredentialSnapshot,
        stream: VecDeque<CredentialSnapshot>,
        supports_header: bool,
    }

    #[async_trait(?Send)]
    impl ClientCredentialProvider for TestProvider {
        fn supports(&self, projection: CredentialProjection) -> bool {
            self.supports_header && projection == CredentialProjection::AuthorizationHeader
        }

        async fn current(&self) -> Result<CredentialSnapshot, CapabilityError> {
            Ok(self.current.clone())
        }

        fn credential_stream(&self) -> CredentialStream {
            Box::pin(stream::iter(self.stream.clone()))
        }
    }

    fn provider(credentials: Vec<CredentialSnapshot>) -> Box<dyn ClientCredentialProvider> {
        Box::new(TestProvider {
            current: credentials[0].clone(),
            stream: credentials.into(),
            supports_header: true,
        })
    }

    /// Scenario: bind Basic credentials to the authorization-header projection.
    /// Guarantees: the projection produces a sensitive, correctly encoded Basic header.
    #[tokio::test]
    async fn basic_credentials_project_to_sensitive_header() {
        let snapshot = CredentialSnapshot::new(
            ClientCredential::basic("user".to_owned(), "password".to_owned()),
            None,
        );
        let mut source = bind(provider(vec![snapshot])).expect("Basic supports headers");
        let projected = source
            .next()
            .await
            .expect("one snapshot")
            .expect("valid header");

        assert_eq!(projected.value(), "Basic dXNlcjpwYXNzd29yZA==");
        assert!(projected.value().is_sensitive());
        assert_eq!(projected.generation(), 1);
    }

    /// Scenario: reject one bearer generation before a refreshed generation arrives.
    /// Guarantees: rejection affects only this projection source and skips only the rejected generation.
    #[tokio::test]
    async fn rejection_waits_for_a_new_generation() {
        let first = CredentialSnapshot::new(ClientCredential::bearer("first".to_owned()), None);
        let second = CredentialSnapshot::new(ClientCredential::bearer("second".to_owned()), None);
        let mut source = bind(provider(vec![first, second])).expect("Bearer supports headers");

        let first = source.next().await.expect("first").expect("valid");
        source.reject(first.generation());
        let second = source.next().await.expect("second").expect("valid");

        assert_eq!(second.value(), "Bearer second");
        assert_eq!(second.generation(), 2);
    }
}
