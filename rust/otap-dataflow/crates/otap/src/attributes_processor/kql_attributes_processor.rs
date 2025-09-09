// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! KQL-based attributes processor for OTAP pipelines.
//!
//! This processor provides attribute transformations for telemetry data using KQL (Kusto Query Language) 
//! configuration. It operates on OTAP Arrow payloads (OtapArrowRecords and OtapArrowBytes) and can convert 
//! OTLP bytes to OTAP for processing.
//!
//! The processor parses KQL expressions and converts them to the same transformation operations
//! supported by the YAML-based attributes processor:
//! - `rename`: Renames an attribute key 
//! - `delete`: Removes an attribute by key
//!
//! Example KQL configurations:
//! 
//! Delete an attribute:
//! ```kql
//! | project-away "db.statement"
//! ```
//!
//! Rename an attribute (using extend + project-away):
//! ```kql
//! | extend "rpc.method" = "http.method" | project-away "http.method"
//! ```
//!
//! You can control which attribute domains are transformed via `apply_to`.
//! Valid values: "signal" (default), "resource", "scope".

use super::common::{
    Action, BaseAttributesProcessor, actions_to_transform, parse_apply_to,
    create_attributes_processor_generic,
};
use crate::{OTAP_PROCESSOR_FACTORIES, pdata::OtapPdata};
use async_trait::async_trait;
use data_engine_expressions::*;
use data_engine_kql_parser::{KqlParser, Parser, ParserOptions};
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

/// URN for the KQL AttributesProcessor
pub const KQL_ATTRIBUTES_PROCESSOR_URN: &str = "urn:otap:processor:kql_attributes_processor";

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Configuration for the KQL AttributesProcessor.
///
/// Accepts KQL query strings for configuring attribute transformations.
pub struct KqlConfig {
    /// KQL query string for attribute transformations
    pub kql: String,

    /// Attribute domains to apply transforms to. Defaults to ["signal"].
    #[serde(default)]
    pub apply_to: Option<Vec<String>>,
}

/// Processor that applies attribute transformations to OTAP attribute batches using KQL configuration.
///
/// This processor parses KQL expressions and converts them to the same transformation operations
/// supported by the YAML-based attributes processor. It reuses the existing Arrow-based
/// attribute transformation infrastructure for efficient processing.
pub struct KqlAttributesProcessor {
    base: BaseAttributesProcessor,
}

impl KqlAttributesProcessor {
    /// Creates a new KqlAttributesProcessor from configuration.
    ///
    /// Parses the KQL query and converts it into the operations
    /// supported by the underlying Arrow attribute transform API.
    #[must_use = "KqlAttributesProcessor creation may fail and return a ConfigError"]
    pub fn from_config(config: &Value) -> Result<Self, ConfigError> {
        let cfg: KqlConfig =
            serde_json::from_value(config.clone()).map_err(|e| ConfigError::InvalidUserConfig {
                error: format!("Failed to parse KqlAttributesProcessor configuration: {e}"),
            })?;
        Self::new(cfg)
    }

    /// Creates a new KqlAttributesProcessor with the given parsed configuration.
    fn new(config: KqlConfig) -> Result<Self, ConfigError> {
        let domains = parse_apply_to(config.apply_to.as_ref());
        
        // Parse the KQL query
        let actions = parse_kql_to_actions(&config.kql)?;
        let transform = actions_to_transform(&actions);

        Ok(Self {
            base: BaseAttributesProcessor::new(transform, domains),
        })
    }
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for KqlAttributesProcessor {
    async fn process(
        &mut self,
        msg: Message<OtapPdata>,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), EngineError> {
        self.base.process(msg, effect_handler).await
    }
}

/// Parse a KQL query string and convert it to a list of Actions.
///
/// This function parses the KQL expression tree and extracts attribute transformation
/// operations that can be represented as rename/delete actions.
fn parse_kql_to_actions(kql: &str) -> Result<Vec<Action>, ConfigError> {
    let options = ParserOptions::default();
    let pipeline = KqlParser::parse_with_options(kql, options)
        .map_err(|errors| ConfigError::InvalidUserConfig {
            error: format!("Failed to parse KQL query: {:?}", errors),
        })?;

    let mut renames: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut deletes = std::collections::HashSet::new();
    
    // Extract transformations from all expressions in the pipeline
    for expression in pipeline.get_expressions() {
        if let DataExpression::Transform(transform) = expression {
            match transform {
                TransformExpression::Set(set_expr) => {
                    // Handle extend operations that set new attributes from existing ones (renames)
                    if let Some((source_key, dest_key)) = extract_rename_keys_from_set(set_expr)? {
                        let _ = renames.insert(source_key, dest_key);
                    }
                }
                TransformExpression::Remove(remove_expr) => {
                    // Handle project-away operations that remove attributes
                    if let Some(key) = extract_delete_key_from_remove(remove_expr)? {
                        let _ = deletes.insert(key);
                    }
                }
                TransformExpression::RemoveMapKeys(remove_keys_expr) => {
                    // Handle removal of map keys
                    extract_delete_keys_from_remove_map_keys(remove_keys_expr, &mut deletes)?;
                }
                TransformExpression::ReduceMap(_) => {
                    // ReduceMap is not directly mappable to simple rename/delete operations
                    // This could be extended in the future
                }
            }
        }
        // Skip DataExpression::Discard and DataExpression::Summary as they're not attribute transformations
    }
    
    let mut actions = Vec::new();
    
    // Process renames: extend operations are always renames
    for (source_key, dest_key) in renames {
        actions.push(Action::Rename {
            source_key: source_key.clone(),
            destination_key: dest_key,
        });
        // If there's a corresponding project-away for the same source key, ignore it
        // (it's redundant since the rename will handle the transformation)
        let _ = deletes.remove(&source_key);
    }
    
    // Process remaining deletes (those not handled by renames)
    for delete_key in deletes {
        actions.push(Action::Delete { key: delete_key });
    }
    
    Ok(actions)
}

/// Extract rename keys from a Set transform expression.
fn extract_rename_keys_from_set(set_expr: &SetTransformExpression) -> Result<Option<(String, String)>, ConfigError> {
    // Look for patterns like: extend "new_key" = "old_key"
    // This would be a simple column reference in the source
    let source = set_expr.get_source();
    let destination = set_expr.get_destination();
    
    // Check if source is a simple column reference
    if let Some(source_key) = extract_column_name_from_scalar(source) {
        if let Some(dest_key) = extract_column_name_from_mutable_value(destination) {
            return Ok(Some((source_key, dest_key)));
        }
    }
    
    Ok(None)
}

/// Extract a delete key from a Remove transform expression.
fn extract_delete_key_from_remove(remove_expr: &RemoveTransformExpression) -> Result<Option<String>, ConfigError> {
    // Look for patterns like: project-away "key_name"
    let target = remove_expr.get_target();
    
    if let Some(key) = extract_column_name_from_mutable_value(target) {
        return Ok(Some(key));
    }
    
    Ok(None)
}

/// Extract delete keys from RemoveMapKeys expressions.
fn extract_delete_keys_from_remove_map_keys(
    remove_keys_expr: &RemoveMapKeysTransformExpression, 
    deletes: &mut std::collections::HashSet<String>
) -> Result<(), ConfigError> {
    match remove_keys_expr {
        RemoveMapKeysTransformExpression::Remove(map_key_list) => {
            // Extract keys to remove
            for key_expr in map_key_list.get_keys() {
                if let Some(key) = extract_column_name_from_scalar(key_expr) {
                    let _ = deletes.insert(key);
                }
            }
        }
        RemoveMapKeysTransformExpression::Retain(_) => {
            // Retain operations are more complex and would require knowing
            // all possible keys to implement as delete operations
            // This could be extended in the future
        }
    }
    Ok(())
}

/// Extract a column name from a scalar expression if it's a simple column reference.
fn extract_column_name_from_scalar(scalar: &ScalarExpression) -> Option<String> {
    match scalar {
        ScalarExpression::Source(source_expr) => {
            // Extract the column name from the value accessor if it's a simple string
            extract_column_name_from_value_accessor(source_expr.get_value_accessor())
        }
        ScalarExpression::Static(static_expr) => {
            // Extract from static string values
            match static_expr {
                StaticScalarExpression::String(string_expr) => {
                    Some(string_expr.get_value().to_string())
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Extract a column name from a mutable value expression.
fn extract_column_name_from_mutable_value(value: &MutableValueExpression) -> Option<String> {
    match value {
        MutableValueExpression::Source(source_expr) => {
            extract_column_name_from_value_accessor(source_expr.get_value_accessor())
        }
        MutableValueExpression::Variable(_) => {
            // Variables are not simple column references
            None
        }
    }
}

/// Extract a column name from a value accessor if it represents a simple field access.
fn extract_column_name_from_value_accessor(accessor: &ValueAccessor) -> Option<String> {
    // For simple field access, we expect a single string selector
    let selectors = accessor.get_selectors();
    if selectors.len() == 1 {
        if let ScalarExpression::Static(StaticScalarExpression::String(string_expr)) = &selectors[0] {
            return Some(string_expr.get_value().to_string());
        }
    }
    None
}

/// Factory function to create a KqlAttributesProcessor.
///
/// Accepts configuration with KQL query strings for attribute transformations.
pub fn create_kql_attributes_processor(
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
            let processor = KqlAttributesProcessor::from_config(config)?;
            Ok(processor.base)
        },
    )
}

/// Register KqlAttributesProcessor as an OTAP processor factory
#[allow(unsafe_code)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
pub static KQL_ATTRIBUTES_PROCESSOR_FACTORY: otap_df_engine::ProcessorFactory<OtapPdata> =
    otap_df_engine::ProcessorFactory {
        name: KQL_ATTRIBUTES_PROCESSOR_URN,
        create: |pipeline_ctx: PipelineContext,
                 node: NodeId,
                 node_config: Arc<NodeUserConfig>,
                 proc_cfg: &ProcessorConfig| {
            create_kql_attributes_processor(pipeline_ctx, node, node_config, proc_cfg)
        },
    };

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attributes_processor::common::{ApplyDomain, test_utils::*};
    use serde_json::json;
    use otel_arrow_rust::proto::opentelemetry::{
        common::v1::{AnyValue, KeyValue},
    };

    #[test]
    fn test_kql_config_from_json_parses_kql_and_apply_to_default() {
        let cfg = json!({
            "kql": "source | project-away db_statement"
        });
        // Should parse without error
        let result = KqlAttributesProcessor::from_config(&cfg);
        if let Err(ref e) = result {
            println!("Error: {:?}", e);
        }
        assert!(result.is_ok());
        
        let processor = result.unwrap();
        // Default apply_to should include Signal
        assert!(processor.base.domains.contains(&ApplyDomain::Signal));
        // and not necessarily Resource/Scope unless specified
        assert!(!processor.base.domains.contains(&ApplyDomain::Resource));
        assert!(!processor.base.domains.contains(&ApplyDomain::Scope));
    }

    #[test]
    fn test_parse_kql_delete() {
        // Test parsing a simple delete operation
        let actions = parse_kql_to_actions("source | project-away test_key");
        if let Err(ref e) = actions {
            println!("Parse error: {:?}", e);
        }
        assert!(actions.is_ok());
        
        let actions = actions.unwrap();
        assert_eq!(actions.len(), 1);
        
        match &actions[0] {
            Action::Delete { key } => assert_eq!(key, "test_key"),
            _ => panic!("Expected Delete action"),
        }
    }

    #[test]
    fn test_parse_kql_rename() {
        // Test parsing a rename operation using just extend (no project-away needed)
        let actions = parse_kql_to_actions("source | extend new_key = old_key");
        if let Err(ref e) = actions {
            println!("Parse error: {:?}", e);
        }
        assert!(actions.is_ok());
        
        let actions = actions.unwrap();
        assert_eq!(actions.len(), 1);
        
        match &actions[0] {
            Action::Rename { source_key, destination_key } => {
                assert_eq!(source_key, "old_key");
                assert_eq!(destination_key, "new_key");
            },
            _ => panic!("Expected Rename action"),
        }
    }

    #[test]
    fn test_parse_kql_extend_with_project_away() {
        // Test that extend + project-away of the same key is still treated as a single rename
        // (project-away is ignored when there's a corresponding extend)
        let actions = parse_kql_to_actions("source | extend new_key = old_key | project-away old_key");
        if let Err(ref e) = actions {
            println!("Parse error: {:?}", e);
        }
        assert!(actions.is_ok());
        
        let actions = actions.unwrap();
        assert_eq!(actions.len(), 1); // Should be only one action (rename), not rename + delete
        
        match &actions[0] {
            Action::Rename { source_key, destination_key } => {
                assert_eq!(source_key, "old_key");
                assert_eq!(destination_key, "new_key");
            },
            _ => panic!("Expected Rename action"),
        }
    }

    #[test]
    fn test_parse_kql_mixed_operations() {
        // Test mixed operations: extend (rename) + project-away without corresponding extend (delete)
        let actions = parse_kql_to_actions("source | extend new_key = old_key | project-away different_key");
        if let Err(ref e) = actions {
            println!("Parse error: {:?}", e);
        }
        assert!(actions.is_ok());
        
        let actions = actions.unwrap();
        assert_eq!(actions.len(), 2); // Should be one rename and one delete
        
        // Find the rename and delete actions
        let mut found_rename = false;
        let mut found_delete = false;
        
        for action in &actions {
            match action {
                Action::Rename { source_key, destination_key } => {
                    assert_eq!(source_key, "old_key");
                    assert_eq!(destination_key, "new_key");
                    found_rename = true;
                },
                Action::Delete { key } => {
                    assert_eq!(key, "different_key");
                    found_delete = true;
                },
                Action::Unsupported => {
                    // Shouldn't happen in this test
                    panic!("Unexpected Unsupported action");
                },
            }
        }
        
        assert!(found_rename, "Should have found a rename action");
        assert!(found_delete, "Should have found a delete action");
    }

    #[test]
    fn test_kql_delete_applies_to_signal_only_by_default() {
        let input = build_logs_with_attrs(
            vec![KeyValue::new("service.name", AnyValue::new_string("test-service"))],
            vec![KeyValue::new("db_statement", AnyValue::new_string("SELECT * FROM users"))],
            vec![
                KeyValue::new("db_statement", AnyValue::new_string("DELETE FROM logs")),
                KeyValue::new("http_method", AnyValue::new_string("GET")),
            ],
        );

        let cfg = json!({
            "kql": "source | project-away db_statement"
        });

        // Use common test execution
        let results = run_attributes_processor_test(
            input,
            cfg,
            KQL_ATTRIBUTES_PROCESSOR_URN,
            create_kql_attributes_processor,
        );

        // Resource should still have service.name (not affected by signal-only delete)
        assert!(has_attr_key(&results.resource_attrs, "service.name"));

        // Scope should still have db_statement (not affected by signal-only delete)
        assert!(has_attr_key(&results.scope_attrs, "db_statement"));

        // Log attrs should have deleted "db_statement" but kept "http_method"
        assert!(!has_attr_key(&results.log_attrs, "db_statement"));
        assert!(has_attr_key(&results.log_attrs, "http_method"));
    }

    #[test]
    fn test_kql_delete_scoped_to_resource_only_logs() {
        // E2E test: KQL delete operation scoped to resource attributes only
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
            "kql": "source | project-away a",
            "apply_to": ["resource"]
        });

        // Use common test execution
        let results = run_attributes_processor_test(
            input,
            cfg,
            KQL_ATTRIBUTES_PROCESSOR_URN,
            create_kql_attributes_processor,
        );

        // Resource 'a' should be deleted; 'r' should remain
        assert!(!has_attr_key(&results.resource_attrs, "a"));
        assert!(has_attr_key(&results.resource_attrs, "r"));

        // Scope 'a' should remain (not affected by resource-only delete)
        assert!(has_attr_key(&results.scope_attrs, "a"));

        // Log 'a' should remain (not affected by resource-only delete)
        assert!(has_attr_key(&results.log_attrs, "a"));
        assert!(has_attr_key(&results.log_attrs, "b"));
    }

     #[test]
    fn test_kql_delete_scoped_to_scope_only_logs() {
        // E2E test: KQL delete operation scoped to scope attributes only
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
            "kql": "source | project-away a",
            "apply_to": ["scope"]
        });

        // Use common test execution
        let results = run_attributes_processor_test(
            input,
            cfg,
            KQL_ATTRIBUTES_PROCESSOR_URN,
            create_kql_attributes_processor,
        );

        // Resource 'a' should remain (not affected by scope-only delete)
        assert!(has_attr_key(&results.resource_attrs, "a"));

        // Scope 'a' should be deleted; 's' should remain
        assert!(!has_attr_key(&results.scope_attrs, "a"));
        assert!(has_attr_key(&results.scope_attrs, "s"));

        // Log 'a' should remain (not affected by scope-only delete)
        assert!(has_attr_key(&results.log_attrs, "a"));
        assert!(has_attr_key(&results.log_attrs, "b"));
    }

    #[test]
    fn test_kql_scoped_to_signal_and_resource() {
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

        // Delete service_version from both resource and signal domains
        let cfg = json!({
            "kql": "source | project-away a",
            "apply_to": ["signal", "resource"]
        });

        // Use common test execution
        let results = run_attributes_processor_test(
            input,
            cfg,
            KQL_ATTRIBUTES_PROCESSOR_URN,
            create_kql_attributes_processor,
        );

        // Resource 'a' should be deleted; 'r' should remain
        assert!(!has_attr_key(&results.resource_attrs, "a"));
        assert!(has_attr_key(&results.resource_attrs, "r"));

        // Scope 'a' should remain
        assert!(has_attr_key(&results.scope_attrs, "a"));
        assert!(has_attr_key(&results.scope_attrs, "s"));

        // Log 'a' should be deleted; 'b' should remain
        assert!(!has_attr_key(&results.log_attrs, "a"));
        assert!(has_attr_key(&results.log_attrs, "b"));
    }

    #[test]
    fn test_kql_extend_only_e2e() {
        // E2E test: Extend-only operation should work as a rename
        let input = build_logs_with_attrs(
            vec![KeyValue::new("service_name", AnyValue::new_string("test-service"))],
            vec![KeyValue::new("library_name", AnyValue::new_string("test-lib"))],
            vec![
                KeyValue::new("http_method", AnyValue::new_string("POST")),
                KeyValue::new("status_code", AnyValue::new_string("200")),
            ],
        );

        // KQL query that renames http_method to rpc_method using just extend (no project-away)
        let cfg = json!({
            "kql": "source | extend rpc_method = http_method"
        });

        // Use common test execution
        let results = run_attributes_processor_test(
            input,
            cfg,
            KQL_ATTRIBUTES_PROCESSOR_URN,
            create_kql_attributes_processor,
        );

        // Resource attrs should be unchanged
        assert!(has_attr_key(&results.resource_attrs, "service_name"));

        // Scope attrs should be unchanged
        assert!(has_attr_key(&results.scope_attrs, "library_name"));

        // Log attrs should have renamed "http_method" to "rpc_method" and kept other attrs
        assert!(!has_attr_key(&results.log_attrs, "http_method"));
        assert!(has_attr_with_string_value(&results.log_attrs, "rpc_method", "POST"));
        assert!(has_attr_key(&results.log_attrs, "status_code"));
    }
}
