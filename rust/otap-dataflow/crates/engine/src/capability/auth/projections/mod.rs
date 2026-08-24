// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Approved consumer projections over typed client credentials.

pub mod authorization_header;
pub mod bearer_access_token;

use super::client_credential_provider::CredentialProjection;
use thiserror::Error;

/// A provider cannot supply the projection requested by a consumer.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("client credential provider does not support projection {projection:?}")]
pub struct UnsupportedProjection {
    /// The projection rejected during node construction.
    pub projection: CredentialProjection,
}
