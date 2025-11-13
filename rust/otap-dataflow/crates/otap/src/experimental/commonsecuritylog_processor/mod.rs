// Copyright The OpenTelemetry Authors SPDX-License-Identifier: Apache-2.0

//! CommonSecurityLog Processor
//!
//! This module provides a processor for producing well-formed CommonSecurityLog
//! records from data collected by the [`syslog_cef_receiver`]. It is designed
//! for compatibility with Microsoft Sentinel's CommonSecurityLog schema.
//!

use async_trait::async_trait;
use linkme::distributed_slice;
use otap_df_config::SignalType;
use otap_df_config::error::Error as ConfigError;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::config::ProcessorConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::error::{Error, ProcessorErrorKind};
use otap_df_engine::local::processor as local;
use otap_df_engine::message::Message;
use otap_df_engine::node::NodeId;
use otap_df_engine::processor::ProcessorWrapper;
use otap_df_pdata::OtapArrowRecords;
use otap_df_pdata::otap::transform::{
    AttributesTransform, RenameTransform, transform_attributes_with_stats,
};
use otap_df_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::OTAP_PROCESSOR_FACTORIES;
use crate::pdata::OtapPdata;

/// URN identifier for the CommonSecurityLog processor
pub const COMMONSECURITYLOG_PROCESSOR_URN: &str = "urn:otel:commonsecuritylog:processor";

/// No configuration is required for this processor. It performs static transformations
/// to map incoming log data to the CommonSecurityLog format.
pub struct Config {}

/// Processor that transforms incoming log data into CommonSecurityLog format
pub struct CommonSecurityLogProcessor {}

fn engine_err(msg: &str) -> Error {
    Error::PdataConversionError {
        error: msg.to_string(),
    }
}

/// Factory function to create a CommonSecurityLog processor
pub fn create_commonsecuritylog_processor(
    _: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    processor_config: &ProcessorConfig,
) -> Result<ProcessorWrapper<OtapPdata>, ConfigError> {
    Ok(ProcessorWrapper::local(
        CommonSecurityLogProcessor::from_config()?,
        node,
        node_config,
        processor_config,
    ))
}

/// Register CommonSecurityLogProcessor as an OTAP processor factory
#[allow(unsafe_code)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
pub static COMMONSECURITYLOG_PROCESSOR_FACTORY: otap_df_engine::ProcessorFactory<OtapPdata> =
    otap_df_engine::ProcessorFactory {
        name: COMMONSECURITYLOG_PROCESSOR_URN,
        create: |pipeline_ctx: PipelineContext,
                 node: NodeId,
                 node_config: Arc<NodeUserConfig>,
                 proc_cfg: &ProcessorConfig| {
            create_commonsecuritylog_processor(pipeline_ctx, node, node_config, proc_cfg)
        },
    };

impl CommonSecurityLogProcessor {
    /// Creates a new CommonSecurityLogProcessor instance
    pub fn new() -> Self {
        CommonSecurityLogProcessor {}
    }

    /// Creates a new CommonSecurityLogProcessor instance from configuration
    pub fn from_config() -> Result<Self, ConfigError> {
        Ok(CommonSecurityLogProcessor {})
    }

    fn transform_to_commonsecuritylog(&self, records: &mut OtapArrowRecords) -> Result<u64, Error> {
        let mut renamed_total: u64 = 0;

        // Perform all well-known renames
        let transform = &AttributesTransform {
            // TODO: Include all required renames
            // Only one included here as an example
            rename: Some(RenameTransform::new(BTreeMap::from_iter([(
                "act".into(),
                "DeviceAction".into(),
            )]))),
            delete: None,
        };
        if let Some(rb) = records.get(ArrowPayloadType::LogAttrs) {
            let (rb, stats) = transform_attributes_with_stats(rb, &transform)
                .map_err(|e| engine_err(&format!("transform_attributes failed: {e}")))?;
            renamed_total += stats.renamed_entries;
            records.set(ArrowPayloadType::LogAttrs, rb);
        }

        // TODO: Additional concat of non-standard fields into AdditionalExtensions attribute

        Ok(renamed_total)
    }
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for CommonSecurityLogProcessor {
    async fn process(
        &mut self,
        msg: Message<OtapPdata>,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        match msg {
            Message::Control(_control) => Ok(()),
            Message::PData(pdata) => {
                let signal = pdata.signal_type();
                let (context, payload) = pdata.into_parts();

                let mut records: OtapArrowRecords = payload.try_into()?;

                let commonsecuritylog_records: OtapArrowRecords =
                    match signal {
                        SignalType::Logs => {
                            match self.transform_to_commonsecuritylog(&mut records) {
                                // TODO: Use stats
                                Ok(_renamed) => records,
                                Err(e) => return Err(e),
                            }
                        }
                        _ => return Err(Error::ProcessorError {
                            processor: effect_handler.processor_id(),
                            kind: ProcessorErrorKind::Other,
                            error:
                                "CommonSecurityLogProcessor only supported for SignalType 'Logs'"
                                    .to_string(),
                            source_detail: String::new(),
                        }),
                    };

                effect_handler
                    .send_message(OtapPdata::new(context, commonsecuritylog_records.into()))
                    .await?;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pdata::OtapPdata;
    use otap_df_engine::context::ControllerContext;
    use otap_df_engine::message::Message;
    use otap_df_engine::testing::{node::test_node, processor::TestRuntime};
    use otap_df_pdata::OtlpProtoBytes;
    use otap_df_pdata::proto::opentelemetry::{
        collector::logs::v1::ExportLogsServiceRequest,
        common::v1::{AnyValue, InstrumentationScope, KeyValue},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
        resource::v1::Resource,
    };
    use otap_df_telemetry::registry::MetricsRegistryHandle;
    use prost::Message as _;
    use serde_json::json;

    fn build_log_with_attrs(log_attrs: Vec<KeyValue>) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest::new(vec![ResourceLogs::new(
            Resource {
                ..Default::default()
            },
            vec![ScopeLogs::new(
                InstrumentationScope {
                    ..Default::default()
                },
                vec![
                    LogRecord::build()
                        .time_unix_nano(1u64)
                        .severity_number(SeverityNumber::Info)
                        .event_name("")
                        .attributes(log_attrs)
                        .finish(),
                ],
            )],
        )])
    }

    #[test]
    fn test_csl_base() {
        let input = build_log_with_attrs(vec![
            KeyValue::new("act", AnyValue::new_string("DeviceActionValue")),
            KeyValue::new(
                "extraextension",
                AnyValue::new_string("ExtraExtensionValue"),
            ),
        ]);

        let cfg = json!({});

        let metrics_registry_handle = MetricsRegistryHandle::new();
        let controller_ctx = ControllerContext::new(metrics_registry_handle);
        let pipeline_ctx =
            controller_ctx.pipeline_context_with("grp".into(), "pipeline".into(), 0, 0);

        let node = test_node("commonsecuritylog-processor-test");
        let rt: TestRuntime<OtapPdata> = TestRuntime::new();
        let mut node_config = NodeUserConfig::new_processor_config(COMMONSECURITYLOG_PROCESSOR_URN);
        node_config.config = cfg;
        let proc = create_commonsecuritylog_processor(
            pipeline_ctx,
            node,
            Arc::new(node_config),
            rt.config(),
        )
        .expect("create processor");
        let phase = rt.set_processor(proc);

        phase
            .run_test(|mut ctx| async move {
                let mut bytes = Vec::new();
                input.encode(&mut bytes).expect("encode");
                let pdata_in =
                    OtapPdata::new_default(OtlpProtoBytes::ExportLogsRequest(bytes).into());
                ctx.process(Message::PData(pdata_in))
                    .await
                    .expect("process");

                let out = ctx.drain_pdata().await;
                let first = out.into_iter().next().expect("one output").payload();

                let otlp_bytes: OtlpProtoBytes = first.try_into().expect("convert to otlp");
                let bytes = match otlp_bytes {
                    OtlpProtoBytes::ExportLogsRequest(b) => b,
                    _ => panic!("unexpected otlp variant"),
                };
                let decoded = ExportLogsServiceRequest::decode(bytes.as_slice()).expect("decode");

                let log_attrs = &decoded.resource_logs[0].scope_logs[0].log_records[0].attributes;
                let expected_log_attrs = vec![
                    KeyValue::new("DeviceAction", AnyValue::new_string("DeviceActionValue")),
                    KeyValue::new(
                        "extraextension",
                        AnyValue::new_string("ExtraExtensionValue"),
                    ),
                ];

                assert_eq!(log_attrs, &expected_log_attrs);
            })
            .validate(|_| async move {});
    }
}
