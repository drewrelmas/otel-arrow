// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Typed application credentials and their provider capability.
//!
//! Providers publish credential material without exposing a generic raw-secret
//! API. Consumers bind the provider through a projection in
//! [`super::projections`], which controls how each credential kind may be used.

use crate::capability::auth::BearerToken;
use crate::capability::error::CapabilityError;
use futures::Stream;
use otel_arrow_dfe_engine_macros::capability;
use secrecy::SecretString;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

/// A supported consumer view over application credential material.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialProjection {
    /// A complete HTTP `Authorization` header value.
    AuthorizationHeader,
    /// A bearer access token for an SDK-specific adapter.
    BearerAccessToken,
}

/// An immutable credential generation published by an auth extension.
#[derive(Clone)]
pub struct CredentialSnapshot {
    pub(super) credential: ClientCredential,
    expires_on: Option<Instant>,
}

impl CredentialSnapshot {
    /// Creates a snapshot for one credential generation.
    #[must_use]
    pub const fn new(credential: ClientCredential, expires_on: Option<Instant>) -> Self {
        Self {
            credential,
            expires_on,
        }
    }

    /// Returns the credential expiry, when known.
    #[must_use]
    pub const fn expires_on(&self) -> Option<Instant> {
        self.expires_on
    }
}

impl Debug for CredentialSnapshot {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialSnapshot")
            .field("credential", &self.credential)
            .field("expires_on", &self.expires_on)
            .finish()
    }
}

/// Typed application credential material.
#[derive(Clone)]
pub struct ClientCredential {
    pub(super) material: CredentialMaterial,
}

impl ClientCredential {
    /// Creates a bearer credential.
    #[must_use]
    pub fn bearer(secret: impl Into<SecretString>) -> Self {
        Self::from_bearer_token(BearerToken::without_expiry(secret))
    }

    /// Creates a bearer credential from the existing token representation.
    #[must_use]
    pub fn from_bearer_token(token: BearerToken) -> Self {
        Self {
            material: CredentialMaterial::Bearer(token),
        }
    }

    /// Creates a Basic credential.
    #[must_use]
    pub fn basic(username: impl Into<SecretString>, password: impl Into<SecretString>) -> Self {
        Self {
            material: CredentialMaterial::Basic {
                username: Arc::new(username.into()),
                password: Arc::new(password.into()),
            },
        }
    }
}

impl Debug for ClientCredential {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let kind = match self.material {
            CredentialMaterial::Bearer(_) => "Bearer",
            CredentialMaterial::Basic { .. } => "Basic",
        };
        f.debug_tuple("ClientCredential").field(&kind).finish()
    }
}

pub(super) enum CredentialMaterial {
    Bearer(BearerToken),
    Basic {
        username: Arc<SecretString>,
        password: Arc<SecretString>,
    },
    // Future application credential material goes here. Each projection must
    // explicitly opt in before consumers can bind it.
}

impl Clone for CredentialMaterial {
    fn clone(&self) -> Self {
        match self {
            Self::Bearer(token) => Self::Bearer(token.clone()),
            Self::Basic { username, password } => Self::Basic {
                username: Arc::clone(username),
                password: Arc::clone(password),
            },
        }
    }
}

/// A per-consumer subscription to the current credential and future refreshes.
pub type CredentialStream = Pin<Box<dyn Stream<Item = CredentialSnapshot> + 'static>>;

/// Provides typed application credentials to projection modules.
#[capability(
    name = "client_credential_provider",
    description = "Provides typed application credentials for approved consumer projections"
)]
pub trait ClientCredentialProvider {
    /// Returns whether this provider supports the requested projection.
    ///
    /// The answer must remain stable for the configured extension lifetime so
    /// projection binding can reject incompatible nodes during construction.
    fn supports(&self, projection: CredentialProjection) -> bool;

    /// Returns the provider's current credential generation.
    async fn current(&self) -> Result<CredentialSnapshot, CapabilityError>;

    /// Subscribes to the current credential followed by future refreshes.
    fn credential_stream(&self) -> CredentialStream;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: render Basic and Bearer credentials with `Debug`.
    /// Guarantees: neither credential secret appears in diagnostic output.
    #[test]
    fn debug_redacts_all_credential_material() {
        let bearer = ClientCredential::bearer("bearer-secret".to_owned());
        let basic = ClientCredential::basic("basic-user".to_owned(), "basic-password".to_owned());

        let rendered = format!("{bearer:?} {basic:?}");
        assert!(!rendered.contains("bearer-secret"));
        assert!(!rendered.contains("basic-user"));
        assert!(!rendered.contains("basic-password"));
    }
}
