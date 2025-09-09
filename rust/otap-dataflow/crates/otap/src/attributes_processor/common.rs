// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Common types and utilities shared between attributes processor implementations.

use async_trait::async_trait;
use otap_df_config::experimental::SignalType;
use otap_df_config::error::Error as ConfigError;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::config::ProcessorConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::error::Error as EngineError;
use otap_df_engine::local::processor as local;
use otap_df_engine::message::Message;
use otap_df_engine::node::NodeId;
use otap_df_engine::processor::ProcessorWrapper;
use otel_arrow_rust::otap::{
    OtapArrowRecords,
    transform::{AttributesTransform, transform_attributes},
};
use otel_arrow_rust::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use crate::pdata::OtapPdata;

/// Actions that can be performed on attributes.
/// This is shared between the YAML and KQL processors.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "lowercase")]
pub enum Action {
    /// Rename an existing attribute key (non-standard; deviates from Go config).
    Rename {
        /// The source key to rename from.
        source_key: String,
        /// The destination key to rename to.
        destination_key: String,
    },

    /// Delete an attribute by key.
    Delete {
        /// The attribute key to delete.
        key: String,
    },

    /// Other actions are accepted for forward-compatibility but ignored.
    /// These variants allow deserialization of Go-style configs without effect.
    #[serde(other)]
    Unsupported,
}

/// Attribute domains that transformations can be applied to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ApplyDomain {
    /// Signal-specific attributes (e.g., span attributes, log attributes).
    Signal,
    /// Resource attributes.
    Resource,
    /// Scope attributes.
    Scope,
}

/// Parse apply_to configuration into a set of domains.
/// Defaults to Signal if empty or None.
pub fn parse_apply_to(apply_to: Option<&Vec<String>>) -> HashSet<ApplyDomain> {
    let mut set = HashSet::new();
    match apply_to {
        None => {
            let _ = set.insert(ApplyDomain::Signal);
        }
        Some(list) => {
            for item in list {
                match item.as_str() {
                    "signal" => {
                        let _ = set.insert(ApplyDomain::Signal);
                    }
                    "resource" => {
                        let _ = set.insert(ApplyDomain::Resource);
                    }
                    "scope" => {
                        let _ = set.insert(ApplyDomain::Scope);
                    }
                    _ => {
                        // Unknown entry: ignore for now; could return config error in future
                    }
                }
            }
            if set.is_empty() {
                let _ = set.insert(ApplyDomain::Signal);
            }
        }
    }
    set
}

/// Convert a list of actions into an AttributesTransform.
pub fn actions_to_transform(actions: &[Action]) -> AttributesTransform {
    let mut renames = BTreeMap::new();
    let mut deletes = BTreeSet::new();

    for action in actions {
        match action {
            Action::Delete { key } => {
                let _ = deletes.insert(key.clone());
            }
            Action::Rename {
                source_key,
                destination_key,
            } => {
                let _ = renames.insert(source_key.clone(), destination_key.clone());
            }
            // Unsupported actions are ignored for now
            Action::Unsupported => {}
        }
    }

    AttributesTransform {
        rename: if renames.is_empty() {
            None
        } else {
            Some(renames)
        },
        delete: if deletes.is_empty() {
            None
        } else {
            Some(deletes)
        },
    }
}

/// Apply transformations to OTAP records for the specified signal type and domains.
#[allow(clippy::result_large_err)]
pub fn apply_transform(
    records: &mut OtapArrowRecords,
    signal: SignalType,
    transform: &AttributesTransform,
    domains: &HashSet<ApplyDomain>,
) -> Result<(), EngineError> {
    let payloads = attrs_payloads(signal, domains);

    // Only apply if we have transforms to apply
    if transform.rename.is_some() || transform.delete.is_some() {
        for payload_ty in payloads {
            if let Some(rb) = records.get(payload_ty).cloned() {
                let rb = transform_attributes(&rb, transform)
                    .map_err(|e| engine_err(&format!("transform_attributes failed: {e}")))?;
                records.set(payload_ty, rb);
            }
        }
    }

    Ok(())
}

/// Get the Arrow payload types that correspond to attributes for a given signal and domains.
fn attrs_payloads(signal: SignalType, domains: &HashSet<ApplyDomain>) -> Vec<ArrowPayloadType> {
    use ArrowPayloadType as A;
    let mut out: Vec<ArrowPayloadType> = Vec::new();
    // Domains are unioned
    if domains.contains(&ApplyDomain::Resource) {
        out.push(A::ResourceAttrs);
    }
    if domains.contains(&ApplyDomain::Scope) {
        out.push(A::ScopeAttrs);
    }
    if domains.contains(&ApplyDomain::Signal) {
        match signal {
            SignalType::Logs => {
                out.push(A::LogAttrs);
            }
            SignalType::Metrics => {
                out.push(A::MetricAttrs);
                out.push(A::NumberDpAttrs);
                out.push(A::HistogramDpAttrs);
                out.push(A::SummaryDpAttrs);
                out.push(A::NumberDpExemplarAttrs);
                out.push(A::HistogramDpExemplarAttrs);
            }
            SignalType::Traces => {
                out.push(A::SpanAttrs);
                out.push(A::SpanEventAttrs);
                out.push(A::SpanLinkAttrs);
            }
        }
    }
    out
}

fn engine_err(msg: &str) -> EngineError {
    EngineError::PdataConversionError {
        error: msg.to_string(),
    }
}

/// Base attributes processor that provides the common processing logic.
///
/// This struct contains the shared fields and behavior that both YAML and KQL
/// attributes processors use. The main difference between processors is how they
/// parse their configuration into the `AttributesTransform` and `domains`.
pub struct BaseAttributesProcessor {
    /// Pre-computed transform to avoid rebuilding per message
    pub transform: AttributesTransform,
    /// Selected attribute domains to transform
    pub domains: HashSet<ApplyDomain>,
}

impl BaseAttributesProcessor {
    /// Create a new BaseAttributesProcessor with the given transform and domains.
    #[must_use]
    pub fn new(transform: AttributesTransform, domains: HashSet<ApplyDomain>) -> Self {
        Self { transform, domains }
    }
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for BaseAttributesProcessor {
    async fn process(
        &mut self,
        msg: Message<OtapPdata>,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), EngineError> {
        match msg {
            Message::Control(_) => Ok(()),
            Message::PData(pdata) => {
                // Fast path: no actions to apply
                if self.transform.rename.is_none() && self.transform.delete.is_none() {
                    return effect_handler
                        .send_message(pdata)
                        .await
                        .map_err(|e| e.into());
                }

                let signal = pdata.signal_type();
                let mut records: OtapArrowRecords = pdata.try_into()?;

                // Apply transform across selected domains
                apply_transform(&mut records, signal, &self.transform, &self.domains)?;

                effect_handler
                    .send_message(records.into())
                    .await
                    .map_err(|e| e.into())
            }
        }
    }
}

/// Factory function template for creating attribute processors.
///
/// This function provides the common boilerplate for creating processor wrappers.
/// The `from_config_fn` parameter should be a function that takes a `&Value` and
/// returns a `Result<BaseAttributesProcessor, ConfigError>`.
pub fn create_attributes_processor_generic<F>(
    _pipeline_ctx: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    processor_config: &ProcessorConfig,
    from_config_fn: F,
) -> Result<ProcessorWrapper<OtapPdata>, ConfigError>
where
    F: FnOnce(&Value) -> Result<BaseAttributesProcessor, ConfigError>,
{
    Ok(ProcessorWrapper::local(
        from_config_fn(&node_config.config)?,
        node,
        node_config,
        processor_config,
    ))
}

#[cfg(test)]
pub mod test_utils {
    //! Shared test utilities for attributes processor tests.
    
    use super::*;
    use crate::pdata::{OtapPdata, OtlpProtoBytes};
    use otap_df_config::node::NodeUserConfig;
    use otap_df_engine::config::ProcessorConfig;
    use otap_df_engine::context::{ControllerContext, PipelineContext};
    use otap_df_engine::message::Message;
    use otap_df_engine::node::NodeId;
    use otap_df_engine::processor::ProcessorWrapper;
    use otap_df_engine::testing::{node::test_node, processor::TestRuntime};
    use otap_df_telemetry::registry::MetricsRegistryHandle;
    use otel_arrow_rust::proto::opentelemetry::{
        collector::logs::v1::ExportLogsServiceRequest,
        common::v1::{InstrumentationScope, KeyValue},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        resource::v1::Resource,
    };
    use prost::Message as ProstMessage;
    use serde_json::Value;
    use std::sync::Arc;

    /// Create a test pipeline context for processor tests.
    pub fn create_test_pipeline_context() -> PipelineContext {
        let metrics_registry_handle = MetricsRegistryHandle::new();
        let controller_ctx = ControllerContext::new(metrics_registry_handle);
        controller_ctx.pipeline_context_with("grp".into(), "pipeline".into(), 0, 0)
    }

    /// Build a test logs request with the specified attribute sets.
    pub fn build_logs_with_attrs(
        res_attrs: Vec<KeyValue>,
        scope_attrs: Vec<KeyValue>,
        log_attrs: Vec<KeyValue>,
    ) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest::new(vec![
            ResourceLogs::build(Resource {
                attributes: res_attrs,
                ..Default::default()
            })
            .scope_logs(vec![
                ScopeLogs::build(InstrumentationScope {
                    attributes: scope_attrs,
                    ..Default::default()
                })
                .log_records(vec![
                    LogRecord {
                        attributes: log_attrs,
                        ..Default::default()
                    }
                ])
                .finish(),
            ])
            .finish(),
        ])
    }

    /// Result type for attribute assertions in tests.
    #[derive(Debug)]
    pub struct AttributeTestResults {
        /// Resource attributes from the processed output.
        pub resource_attrs: Vec<KeyValue>,
        /// Scope attributes from the processed output.
        pub scope_attrs: Vec<KeyValue>,
        /// Log attributes from the processed output.
        pub log_attrs: Vec<KeyValue>,
    }

    /// Common test execution pattern for attribute processors.
    /// 
    /// This function handles the boilerplate of:
    /// 1. Creating a processor from config
    /// 2. Setting up the test runtime
    /// 3. Processing the input message
    /// 4. Extracting and decoding the output
    /// 5. Returning structured attribute results for assertions
    pub fn run_attributes_processor_test<F>(
        input: ExportLogsServiceRequest,
        config: Value,
        processor_urn: &'static str,
        processor_factory: F,
    ) -> AttributeTestResults 
    where
        F: FnOnce(PipelineContext, NodeId, Arc<NodeUserConfig>, &ProcessorConfig) 
            -> Result<ProcessorWrapper<OtapPdata>, ConfigError>,
    {
        // Create pipeline context and test runtime
        let pipeline_ctx = create_test_pipeline_context();
        let node = test_node("attributes-processor-test");
        let rt: TestRuntime<OtapPdata> = TestRuntime::new();
        let mut node_config = NodeUserConfig::new_processor_config(processor_urn);
        node_config.config = config;
        
        let proc = processor_factory(pipeline_ctx, node, Arc::new(node_config), rt.config())
            .expect("create processor");
        let phase = rt.set_processor(proc);

        // Use Arc to share the result between closure and outer scope
        let result = Arc::new(std::sync::Mutex::new(None));
        let result_clone = result.clone();
        
        phase
            .run_test(move |mut ctx| async move {
                let mut bytes = Vec::new();
                input.encode(&mut bytes).expect("encode");
                let pdata_in: OtapPdata = OtlpProtoBytes::ExportLogsRequest(bytes).into();
                ctx.process(Message::PData(pdata_in))
                    .await
                    .expect("process");

                // Capture output
                let out = ctx.drain_pdata().await;
                let first = out.into_iter().next().expect("one output");

                // Convert output to OTLP bytes for assertions
                let otlp_bytes: OtlpProtoBytes = first.try_into().expect("convert to otlp");
                let bytes = match otlp_bytes {
                    OtlpProtoBytes::ExportLogsRequest(b) => b,
                    _ => panic!("unexpected otlp variant"),
                };
                let decoded = ExportLogsServiceRequest::decode(bytes.as_slice()).expect("decode");

                // Extract attribute data for assertions
                let resource_attrs = decoded.resource_logs[0]
                    .resource
                    .as_ref()
                    .unwrap()
                    .attributes
                    .clone();
                
                let scope_attrs = decoded.resource_logs[0].scope_logs[0]
                    .scope
                    .as_ref()
                    .unwrap()
                    .attributes
                    .clone();
                
                let log_attrs = decoded.resource_logs[0].scope_logs[0].log_records[0]
                    .attributes
                    .clone();

                let test_result = AttributeTestResults {
                    resource_attrs,
                    scope_attrs,
                    log_attrs,
                };
                
                *result_clone.lock().unwrap() = Some(test_result);
            })
            .validate(|_| async move {});

        // Extract the result
        Arc::try_unwrap(result)
            .unwrap()
            .into_inner()
            .unwrap()
            .expect("test should have produced results")
    }

    /// Helper to check if an attribute with a specific key exists in a collection.
    pub fn has_attr_key(attrs: &[KeyValue], key: &str) -> bool {
        attrs.iter().any(|kv| kv.key == key)
    }

    /// Helper to check if an attribute with a specific key and string value exists.
    pub fn has_attr_with_string_value(attrs: &[KeyValue], key: &str, expected_value: &str) -> bool {
        attrs.iter().any(|kv| {
            if kv.key != key { return false; }
            match kv.value.as_ref().and_then(|v| v.value.as_ref()) {
                Some(otel_arrow_rust::proto::opentelemetry::common::v1::any_value::Value::StringValue(s)) => s == expected_value,
                _ => false,
            }
        })
    }
}
