// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Attributes processor implementations for OTAP pipelines.
//!
//! This module provides both YAML-based and KQL-based attribute transformations for telemetry data.
//! Both processors operate on OTAP Arrow payloads and share common transformation infrastructure.

pub mod common;
pub mod kql_attributes_processor;
pub mod attributes_processor;

// Re-export the main processor types and constants
pub use kql_attributes_processor::{KqlAttributesProcessor, KqlConfig, KQL_ATTRIBUTES_PROCESSOR_URN};
pub use attributes_processor::{AttributesProcessor, Config as YamlConfig, ATTRIBUTES_PROCESSOR_URN};

// Re-export shared types
pub use common::{Action, ApplyDomain, BaseAttributesProcessor};
