// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! YAML-based attributes processor for OTAP pipelines.
//!
//! This processor provides attribute transformations for telemetry data using YAML configuration.
//! It operates on OTAP Arrow payloads (OtapArrowRecords and OtapArrowBytes) and can convert OTLP
//! bytes to OTAP for processing.
//!
//! Supported actions (current subset):
//! - `rename`: Renames an attribute key (non-standard deviation from the Go collector).
//! - `delete`: Removes an attribute by key.
//!
//! Unsupported actions are ignored if present in the config:
//! `insert`, `upsert`, `update` (value update), `hash`, `extract`, `convert`.
//! We may add support for them later.
//!
//! Example configuration (YAML):
//! You can optionally scope the transformation using `apply_to`. Valid values: signal, resource, scope.
//! If omitted, defaults to [signal].
//! ```yaml
//! actions:
//!   - action: "rename"
//!     source_key: "http.method"
//!     destination_key: "rpc.method"       # Renames http.method to rpc.method
//!   - key: "db.statement"
//!     action: "delete"       # Removes db.statement attribute
//!   # apply_to: ["signal", "resource"]  # Optional; defaults to ["signal"]
//! ```
//!
//! Implementation uses otel_arrow_rust::otap::transform::transform_attributes for
//! efficient batch processing of Arrow record batches.

use super::common::{
    Action, BaseAttributesProcessor, actions_to_transform, parse_apply_to,
    create_attributes_processor_generic,
};
use crate::{OTAP_PROCESSOR_FACTORIES, pdata::OtapPdata};
use async_trait::async_trait;
use linkme::distributed_slice;
use otap_df_config::error::Error as ConfigError;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::config::ProcessorConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::error::Error as EngineError;
use otap_df_engine::local::processor as local;
use otap_df_engine::message::Message;
use otap_df_engine::node::NodeId;
use otap_df_engine::processor::ProcessorWrapper;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// URN for the AttributesProcessor
pub const ATTRIBUTES_PROCESSOR_URN: &str = "urn:otap:processor:attributes_processor";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Configuration for the AttributesProcessor.
///
/// Accepts configuration in the same format as the OpenTelemetry Collector's attributes processor.
/// Supported actions: rename (deviation), delete. Others are ignored.
///
/// You can control which attribute domains are transformed via `apply_to`.
/// Valid values: "signal" (default), "resource", "scope".
pub struct Config {
    /// List of actions to apply in order.
    #[serde(default)]
    pub actions: Vec<Action>,

    /// Attribute domains to apply transforms to. Defaults to ["signal"].
    #[serde(default)]
    pub apply_to: Option<Vec<String>>,
}

/// Processor that applies attribute transformations to OTAP attribute batches.
///
/// Implements the OpenTelemetry Collector's attributes processor functionality using
/// efficient Arrow operations. Supports `update` (rename) and `delete` operations
/// across all attribute types (resource, scope, and signal-specific attributes)
/// for logs, metrics, and traces telemetry.
pub struct AttributesProcessor {
    base: BaseAttributesProcessor,
}

impl AttributesProcessor {
    /// Creates a new AttributesProcessor from configuration.
    ///
    /// Transforms the Go collector-style configuration into the operations
    /// supported by the underlying Arrow attribute transform API.
    #[must_use = "AttributesProcessor creation may fail and return a ConfigError"]
    pub fn from_config(config: &Value) -> Result<Self, ConfigError> {
        let cfg: Config =
            serde_json::from_value(config.clone()).map_err(|e| ConfigError::InvalidUserConfig {
                error: format!("Failed to parse AttributesProcessor configuration: {e}"),
            })?;
        Ok(Self::new(cfg))
    }

    /// Creates a new AttributesProcessor with the given parsed configuration.
    #[must_use]
    fn new(config: Config) -> Self {
        let domains = parse_apply_to(config.apply_to.as_ref());
        let transform = actions_to_transform(&config.actions);

        // TODO: Optimize action composition into a valid AttributesTransform that
        // still reflects the user's intended semantics. Consider:
        // - detecting and collapsing simple rename chains (e.g., a->b, b->c => a->c)
        // - detecting cycles or duplicate destinations and falling back to serial
        //   application of transforms when a composed map would be invalid.
        // For now, we compose a single transform and let transform_attributes
        // enforce validity (which may error for conflicting maps).
        Self {
            base: BaseAttributesProcessor::new(transform, domains),
        }
    }
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for AttributesProcessor {
    async fn process(
        &mut self,
        msg: Message<OtapPdata>,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), EngineError> {
        self.base.process(msg, effect_handler).await
    }
}

/// Factory function to create an AttributesProcessor.
///
/// Accepts configuration in OpenTelemetry Collector attributes processor format.
/// See the module documentation for configuration examples and supported operations.
pub fn create_attributes_processor(
    pipeline_ctx: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    processor_config: &ProcessorConfig,
) -> Result<ProcessorWrapper<OtapPdata>, ConfigError> {
    create_attributes_processor_generic(
        pipeline_ctx,
        node,
        node_config,
        processor_config,
        |config| {
            let processor = AttributesProcessor::from_config(config)?;
            Ok(processor.base)
        },
    )
}

/// Register AttributesProcessor as an OTAP processor factory
#[allow(unsafe_code)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
pub static ATTRIBUTES_PROCESSOR_FACTORY: otap_df_engine::ProcessorFactory<OtapPdata> =
    otap_df_engine::ProcessorFactory {
        name: ATTRIBUTES_PROCESSOR_URN,
        create: |pipeline_ctx: PipelineContext,
                 node: NodeId,
                 node_config: Arc<NodeUserConfig>,
                 proc_cfg: &ProcessorConfig| {
            create_attributes_processor(pipeline_ctx, node, node_config, proc_cfg)
        },
    };

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attributes_processor::common::{test_utils::*, ApplyDomain};
    use crate::pdata::{OtapPdata, OtlpProtoBytes};
    use otap_df_engine::message::Message;
    use otap_df_engine::testing::{node::test_node, processor::TestRuntime};
    use prost::Message as _;
    use serde_json::json;

    use otap_df_engine::context::ControllerContext;
    use otap_df_telemetry::registry::MetricsRegistryHandle;
    use otel_arrow_rust::proto::opentelemetry::{
        collector::logs::v1::ExportLogsServiceRequest,
        common::v1::{AnyValue, KeyValue},
    };

    #[test]
    fn test_config_from_json_parses_actions_and_apply_to_default() {
        let cfg = json!({
            "actions": [
                {"action": "rename", "source_key": "a", "destination_key": "b"},
                {"action": "delete", "key": "x"}
            ]
        });
        let parsed = AttributesProcessor::from_config(&cfg).expect("config parse");
        assert!(parsed.base.transform.rename.is_some());
        assert!(parsed.base.transform.delete.is_some());
        // default apply_to should include Signal
        assert!(parsed.base.domains.contains(&ApplyDomain::Signal));
        // and not necessarily Resource/Scope unless specified
        assert!(!parsed.base.domains.contains(&ApplyDomain::Resource));
        assert!(!parsed.base.domains.contains(&ApplyDomain::Scope));
    }

    #[test]
    fn test_rename_applies_to_signal_only_by_default() {
        // Prepare input with same key present in resource, scope, and log attrs
        let input = build_logs_with_attrs(
            vec![KeyValue::new("a", AnyValue::new_string("rv"))],
            vec![KeyValue::new("a", AnyValue::new_string("sv"))],
            vec![
                KeyValue::new("a", AnyValue::new_string("lv")),
                KeyValue::new("b", AnyValue::new_string("keep")),
            ],
        );

        let cfg = json!({
            "actions": [
                {"action": "rename", "source_key": "a", "destination_key": "b"}
            ]
        });

        // Create a proper pipeline context for the test
        let metrics_registry_handle = MetricsRegistryHandle::new();
        let controller_ctx = ControllerContext::new(metrics_registry_handle);
        let pipeline_ctx =
            controller_ctx.pipeline_context_with("grp".into(), "pipeline".into(), 0, 0);

        // Set up processor test runtime and run one message
        let node = test_node("attributes-processor-test");
        let rt: TestRuntime<OtapPdata> = TestRuntime::new();
        let mut node_config = NodeUserConfig::new_processor_config(ATTRIBUTES_PROCESSOR_URN);
        node_config.config = cfg;
        let proc =
            create_attributes_processor(pipeline_ctx, node, Arc::new(node_config), rt.config())
                .expect("create processor");
        let phase = rt.set_processor(proc);

        phase
            .run_test(|mut ctx| async move {
                let mut bytes = Vec::new();
                input.encode(&mut bytes).expect("encode");
                let pdata_in: OtapPdata = OtlpProtoBytes::ExportLogsRequest(bytes).into();
                ctx.process(Message::PData(pdata_in))
                    .await
                    .expect("process");

                // capture output
                let out = ctx.drain_pdata().await;
                let first = out.into_iter().next().expect("one output");

                // Convert output to OTLP bytes for easy assertions
                let otlp_bytes: OtlpProtoBytes = first.try_into().expect("convert to otlp");
                let bytes = match otlp_bytes {
                    OtlpProtoBytes::ExportLogsRequest(b) => b,
                    _ => panic!("unexpected otlp variant"),
                };
                let decoded = ExportLogsServiceRequest::decode(bytes.as_slice()).expect("decode");

                // Resource should still have key "a"
                let res_attrs = &decoded.resource_logs[0]
                    .resource
                    .as_ref()
                    .unwrap()
                    .attributes;
                assert!(res_attrs.iter().any(|kv| kv.key == "a"));
                assert!(!res_attrs.iter().any(|kv| kv.key == "b"));

                // Scope should still have key "a"
                let scope_attrs = &decoded.resource_logs[0].scope_logs[0]
                    .scope
                    .as_ref()
                    .unwrap()
                    .attributes;
                assert!(scope_attrs.iter().any(|kv| kv.key == "a"));
                assert!(!scope_attrs.iter().any(|kv| kv.key == "b"));

                // Log attrs should have renamed to "b"
                let log_attrs = &decoded.resource_logs[0].scope_logs[0].log_records[0].attributes;
                assert!(log_attrs.iter().any(|kv| kv.key == "b"));
                assert!(!log_attrs.iter().any(|kv| kv.key == "a"));
            })
            .validate(|_| async move {});
    }

    #[test]
    fn test_delete_applies_to_signal_only_by_default() {
        // Prepare input with same key present in resource, scope, and log attrs
        let input = build_logs_with_attrs(
            vec![KeyValue::new("a", AnyValue::new_string("rv"))],
            vec![KeyValue::new("a", AnyValue::new_string("sv"))],
            vec![
                KeyValue::new("a", AnyValue::new_string("lv")),
                KeyValue::new("b", AnyValue::new_string("keep")),
            ],
        );

        let cfg = json!({
            "actions": [
                {"action": "delete", "key": "a"}
            ]
        });

        // Create a proper pipeline context for the test
        let metrics_registry_handle = MetricsRegistryHandle::new();
        let controller_ctx = ControllerContext::new(metrics_registry_handle);
        let pipeline_ctx =
            controller_ctx.pipeline_context_with("grp".into(), "pipeline".into(), 0, 0);

        let node = test_node("attributes-processor-delete-test");
        let rt: TestRuntime<OtapPdata> = TestRuntime::new();
        let mut node_config = NodeUserConfig::new_processor_config(ATTRIBUTES_PROCESSOR_URN);
        node_config.config = cfg;
        let proc =
            create_attributes_processor(pipeline_ctx, node, Arc::new(node_config), rt.config())
                .expect("create processor");
        let phase = rt.set_processor(proc);

        phase
            .run_test(|mut ctx| async move {
                let mut bytes = Vec::new();
                input.encode(&mut bytes).expect("encode");
                let pdata_in: OtapPdata = OtlpProtoBytes::ExportLogsRequest(bytes).into();
                ctx.process(Message::PData(pdata_in))
                    .await
                    .expect("process");

                let out = ctx.drain_pdata().await;
                let first = out.into_iter().next().expect("one output");

                let otlp_bytes: OtlpProtoBytes = first.try_into().expect("convert to otlp");
                let bytes = match otlp_bytes {
                    OtlpProtoBytes::ExportLogsRequest(b) => b,
                    _ => panic!("unexpected otlp variant"),
                };
                let decoded = ExportLogsServiceRequest::decode(bytes.as_slice()).expect("decode");

                // Resource should still have key "a"
                let res_attrs = &decoded.resource_logs[0]
                    .resource
                    .as_ref()
                    .unwrap()
                    .attributes;
                assert!(res_attrs.iter().any(|kv| kv.key == "a"));

                // Scope should still have key "a"
                let scope_attrs = &decoded.resource_logs[0].scope_logs[0]
                    .scope
                    .as_ref()
                    .unwrap()
                    .attributes;
                assert!(scope_attrs.iter().any(|kv| kv.key == "a"));
                // Log attrs should have deleted "a" but still contain other keys
                let log_attrs = &decoded.resource_logs[0].scope_logs[0].log_records[0].attributes;
                assert!(!log_attrs.iter().any(|kv| kv.key == "a"));
                assert!(log_attrs.iter().any(|kv| {
                    if kv.key != "b" { return false; }
                    match kv.value.as_ref().and_then(|v| v.value.as_ref()) {
                        Some(otel_arrow_rust::proto::opentelemetry::common::v1::any_value::Value::StringValue(s)) => s == "keep",
                        _ => false,
                    }
                }));
            })
            .validate(|_| async move {});
    }

    #[test]
    fn test_delete_scoped_to_resource_only_logs() {
        // Resource has 'a', scope has 'a', log has 'a' and another key to keep batch non-empty
        let input = build_logs_with_attrs(
            vec![
                KeyValue::new("a", AnyValue::new_string("rv")),
                KeyValue::new("r", AnyValue::new_string("keep")),
            ],
            vec![KeyValue::new("a", AnyValue::new_string("sv"))],
            vec![
                KeyValue::new("a", AnyValue::new_string("lv")),
                KeyValue::new("b", AnyValue::new_string("keep")),
            ],
        );

        let cfg = json!({
            "actions": [ {"action": "delete", "key": "a"} ],
            "apply_to": ["resource"]
        });

        // Create a proper pipeline context for the test
        let metrics_registry_handle = MetricsRegistryHandle::new();
        let controller_ctx = ControllerContext::new(metrics_registry_handle);
        let pipeline_ctx =
            controller_ctx.pipeline_context_with("grp".into(), "pipeline".into(), 0, 0);

        let node = test_node("attributes-processor-delete-resource");
        let rt: TestRuntime<OtapPdata> = TestRuntime::new();
        let mut node_config = NodeUserConfig::new_processor_config(ATTRIBUTES_PROCESSOR_URN);
        node_config.config = cfg;
        let proc =
            create_attributes_processor(pipeline_ctx, node, Arc::new(node_config), rt.config())
                .expect("create processor");
        let phase = rt.set_processor(proc);

        phase
            .run_test(|mut ctx| async move {
                let mut bytes = Vec::new();
                input.encode(&mut bytes).expect("encode");
                let pdata_in: OtapPdata = OtlpProtoBytes::ExportLogsRequest(bytes).into();
                ctx.process(Message::PData(pdata_in))
                    .await
                    .expect("process");

                let out = ctx.drain_pdata().await;
                let first = out.into_iter().next().expect("one output");
                let otlp_bytes: OtlpProtoBytes = first.try_into().expect("convert to otlp");
                let bytes = match otlp_bytes {
                    OtlpProtoBytes::ExportLogsRequest(b) => b,
                    _ => panic!("unexpected otlp variant"),
                };
                let decoded = ExportLogsServiceRequest::decode(bytes.as_slice()).expect("decode");

                // Resource 'a' should be deleted
                let res_attrs = &decoded.resource_logs[0]
                    .resource
                    .as_ref()
                    .unwrap()
                    .attributes;
                assert!(!res_attrs.iter().any(|kv| kv.key == "a"));
                // Scope 'a' should remain
                let scope_attrs = &decoded.resource_logs[0].scope_logs[0]
                    .scope
                    .as_ref()
                    .unwrap()
                    .attributes;
                assert!(scope_attrs.iter().any(|kv| kv.key == "a"));
                // Log 'a' should remain
                let log_attrs = &decoded.resource_logs[0].scope_logs[0].log_records[0].attributes;
                assert!(log_attrs.iter().any(|kv| kv.key == "a"));
            })
            .validate(|_| async move {});
    }

    #[test]
    fn test_delete_scoped_to_scope_only_logs() {
        // Resource has 'a', scope has 'a' plus another key, log has 'a' and another key to keep batches non-empty
        let input = build_logs_with_attrs(
            vec![KeyValue::new("a", AnyValue::new_string("rv"))],
            vec![
                KeyValue::new("a", AnyValue::new_string("sv")),
                KeyValue::new("s", AnyValue::new_string("keep")),
            ],
            vec![
                KeyValue::new("a", AnyValue::new_string("lv")),
                KeyValue::new("b", AnyValue::new_string("keep")),
            ],
        );

        let cfg = json!({
            "actions": [ {"action": "delete", "key": "a"} ],
            "apply_to": ["scope"]
        });

        // Create a proper pipeline context for the test
        let metrics_registry_handle = MetricsRegistryHandle::new();
        let controller_ctx = ControllerContext::new(metrics_registry_handle);
        let pipeline_ctx =
            controller_ctx.pipeline_context_with("grp".into(), "pipeline".into(), 0, 0);

        let node = test_node("attributes-processor-delete-scope");
        let rt: TestRuntime<OtapPdata> = TestRuntime::new();
        let mut node_config = NodeUserConfig::new_processor_config(ATTRIBUTES_PROCESSOR_URN);
        node_config.config = cfg;
        let proc =
            create_attributes_processor(pipeline_ctx, node, Arc::new(node_config), rt.config())
                .expect("create processor");
        let phase = rt.set_processor(proc);

        phase
            .run_test(|mut ctx| async move {
                let mut bytes = Vec::new();
                input.encode(&mut bytes).expect("encode");
                let pdata_in: OtapPdata = OtlpProtoBytes::ExportLogsRequest(bytes).into();
                ctx.process(Message::PData(pdata_in))
                    .await
                    .expect("process");

                let out = ctx.drain_pdata().await;
                let first = out.into_iter().next().expect("one output");
                let otlp_bytes: OtlpProtoBytes = first.try_into().expect("convert to otlp");
                let bytes = match otlp_bytes {
                    OtlpProtoBytes::ExportLogsRequest(b) => b,
                    _ => panic!("unexpected otlp variant"),
                };
                let decoded = ExportLogsServiceRequest::decode(bytes.as_slice()).expect("decode");

                // Resource 'a' should remain
                let res_attrs = &decoded.resource_logs[0]
                    .resource
                    .as_ref()
                    .unwrap()
                    .attributes;
                assert!(res_attrs.iter().any(|kv| kv.key == "a"));
                // Scope 'a' should be deleted
                let scope_attrs = &decoded.resource_logs[0].scope_logs[0]
                    .scope
                    .as_ref()
                    .unwrap()
                    .attributes;
                assert!(!scope_attrs.iter().any(|kv| kv.key == "a"));
                // Log 'a' should remain
                let log_attrs = &decoded.resource_logs[0].scope_logs[0].log_records[0].attributes;
                assert!(log_attrs.iter().any(|kv| kv.key == "a"));
            })
            .validate(|_| async move {});
    }

    #[test]
    fn test_delete_scoped_to_signal_and_resource() {
        // Resource has 'a' and 'r', scope has 'a' and 's', log has 'a' and 'b'
        // Deleting 'a' for [signal, resource] should remove it from resource and logs, keep in scope.
        let input = build_logs_with_attrs(
            vec![
                KeyValue::new("a", AnyValue::new_string("rv")),
                KeyValue::new("r", AnyValue::new_string("keep")),
            ],
            vec![
                KeyValue::new("a", AnyValue::new_string("sv")),
                KeyValue::new("s", AnyValue::new_string("keep")),
            ],
            vec![
                KeyValue::new("a", AnyValue::new_string("lv")),
                KeyValue::new("b", AnyValue::new_string("keep")),
            ],
        );

        let cfg = json!({
            "actions": [ {"action": "delete", "key": "a"} ],
            "apply_to": ["signal", "resource"]
        });

        // Create a proper pipeline context for the test
        let metrics_registry_handle = MetricsRegistryHandle::new();
        let controller_ctx = ControllerContext::new(metrics_registry_handle);
        let pipeline_ctx =
            controller_ctx.pipeline_context_with("grp".into(), "pipeline".into(), 0, 0);

        let node = test_node("attributes-processor-delete-signal-and-resource");
        let rt: TestRuntime<OtapPdata> = TestRuntime::new();
        let mut node_config = NodeUserConfig::new_processor_config(ATTRIBUTES_PROCESSOR_URN);
        node_config.config = cfg;
        let proc =
            create_attributes_processor(pipeline_ctx, node, Arc::new(node_config), rt.config())
                .expect("create processor");
        let phase = rt.set_processor(proc);

        phase
            .run_test(|mut ctx| async move {
                let mut bytes = Vec::new();
                input.encode(&mut bytes).expect("encode");
                let pdata_in: OtapPdata = OtlpProtoBytes::ExportLogsRequest(bytes).into();
                ctx.process(Message::PData(pdata_in))
                    .await
                    .expect("process");

                let out = ctx.drain_pdata().await;
                let first = out.into_iter().next().expect("one output");
                let otlp_bytes: OtlpProtoBytes = first.try_into().expect("convert to otlp");
                let bytes = match otlp_bytes {
                    OtlpProtoBytes::ExportLogsRequest(b) => b,
                    _ => panic!("unexpected otlp variant"),
                };
                let decoded = ExportLogsServiceRequest::decode(bytes.as_slice()).expect("decode");

                // Resource 'a' should be deleted; 'r' should remain
                let res_attrs = &decoded.resource_logs[0]
                    .resource
                    .as_ref()
                    .unwrap()
                    .attributes;
                assert!(!res_attrs.iter().any(|kv| kv.key == "a"));
                assert!(res_attrs.iter().any(|kv| kv.key == "r"));

                // Scope 'a' should remain
                let scope_attrs = &decoded.resource_logs[0].scope_logs[0]
                    .scope
                    .as_ref()
                    .unwrap()
                    .attributes;
                assert!(scope_attrs.iter().any(|kv| kv.key == "a"));
                assert!(scope_attrs.iter().any(|kv| kv.key == "s"));

                // Log 'a' should be deleted; 'b' should remain
                let log_attrs = &decoded.resource_logs[0].scope_logs[0].log_records[0].attributes;
                assert!(!log_attrs.iter().any(|kv| kv.key == "a"));
                assert!(log_attrs.iter().any(|kv| kv.key == "b"));
            })
            .validate(|_| async move {});
    }
}
