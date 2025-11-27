// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{
    cell::{Ref, RefCell},
    collections::{HashMap, HashSet},
};

use data_engine_expressions::*;

use crate::{ParserError, ParserMapSchema, ParserOptions};

pub struct ParserState {
    source_map_schema: RefCell<Option<ParserMapSchema>>,
    summary_map_schema: RefCell<Option<ParserMapSchema>>,
    attached_data_names: HashSet<Box<str>>,
    global_variable_names: RefCell<HashSet<Box<str>>>,
    variable_names: RefCell<HashSet<Box<str>>>,
    constants: RefCell<HashMap<Box<str>, (usize, ValueType)>>,
    pipeline_builder: RefCell<PipelineExpressionBuilder>,
}

impl ParserState {
    pub fn new(query: &str) -> ParserState {
        ParserState::new_with_options(query, ParserOptions::new())
    }

    pub fn new_with_options(query: &str, options: ParserOptions) -> ParserState {
        Self {
            source_map_schema: RefCell::new(options.source_map_schema),
            summary_map_schema: RefCell::new(options.summary_map_schema),
            attached_data_names: options.attached_data_names,
            global_variable_names: RefCell::new(HashSet::new()),
            variable_names: RefCell::new(HashSet::new()),
            constants: RefCell::new(HashMap::new()),
            pipeline_builder: RefCell::new(PipelineExpressionBuilder::new(query)),
        }
    }

    pub fn push_global_variable(&self, name: &str, value: ScalarExpression) {
        self.pipeline_builder
            .borrow_mut()
            .push_global_variable(name, value);
        self.global_variable_names.borrow_mut().insert(name.into());
    }

    pub fn push_expression(&self, expression: DataExpression) {
        self.pipeline_builder
            .borrow_mut()
            .push_expression(expression)
    }

    pub fn build(self) -> Result<PipelineExpression, Vec<ParserError>> {
        self.pipeline_builder
            .into_inner()
            .build()
            .map_err(|e| e.iter().map(ParserError::from).collect())
    }

    /// Extract the current source schema. Useful for testing to inspect the mutated schema.
    pub fn extract_source_schema(&self) -> Option<ParserMapSchema> {
        self.source_map_schema.borrow().clone()
    }

    /// Extract the current summary schema. Useful for testing to inspect the mutated schema.
    pub fn extract_summary_schema(&self) -> Option<ParserMapSchema> {
        self.summary_map_schema.borrow().clone()
    }
}

impl ParserScope for ParserState {
    fn get_pipeline(&self) -> Ref<'_, PipelineExpression> {
        Ref::map(self.pipeline_builder.borrow(), |v| v.as_ref())
    }

    fn get_source_schema(&self) -> Option<Ref<'_, ParserMapSchema>> {
        let borrow = self.source_map_schema.borrow();
        if borrow.is_some() {
            Some(Ref::map(borrow, |opt| opt.as_ref().unwrap()))
        } else {
            None
        }
    }

    fn get_summary_schema(&self) -> Option<Ref<'_, ParserMapSchema>> {
        let borrow = self.summary_map_schema.borrow();
        if borrow.is_some() {
            Some(Ref::map(borrow, |opt| opt.as_ref().unwrap()))
        } else {
            None
        }
    }

    fn is_well_defined_identifier(&self, name: &str) -> bool {
        name == "source"
            || self.is_attached_data_defined(name)
            || self.is_variable_defined(name)
            || self.get_constant_id(name).is_some()
    }

    fn is_attached_data_defined(&self, name: &str) -> bool {
        self.attached_data_names.contains(name)
    }

    fn is_variable_defined(&self, name: &str) -> bool {
        self.variable_names.borrow().contains(name)
            || self.global_variable_names.borrow().contains(name)
    }

    fn get_constant_id(&self, name: &str) -> Option<(usize, ValueType)> {
        self.constants.borrow().get(name).cloned()
    }

    fn get_constant_name(&self, id: usize) -> Option<(Box<str>, ValueType)> {
        self.constants
            .borrow()
            .iter()
            .find(|(_, v)| v.0 == id)
            .map(|(k, v)| (k.clone(), v.1.clone()))
    }

    fn push_variable_name(&self, name: &str) {
        self.variable_names.borrow_mut().insert(name.into());
    }

    fn push_constant(&self, name: &str, value: StaticScalarExpression) -> usize {
        let value_type = value.get_value_type();
        let constant_id = self.pipeline_builder.borrow_mut().push_constant(value);

        self.constants
            .borrow_mut()
            .insert(name.into(), (constant_id, value_type));

        constant_id
    }

    fn create_scope<'a>(&'a self, options: ParserOptions) -> ParserStateScope<'a> {
        ParserStateScope {
            pipeline_builder: &self.pipeline_builder,
            source_map_schema: RefCell::new(options.source_map_schema),
            summary_map_schema: RefCell::new(options.summary_map_schema),
            attached_data_names: options.attached_data_names,
            global_variable_names: &self.global_variable_names,
            variable_names: RefCell::new(HashSet::new()),
            constants: &self.constants,
        }
    }

    fn add_source_key(&self, name: &str, schema: crate::ParserMapKeySchema) -> Result<(), ParserError> {
        if let Some(ref mut map_schema) = *self.source_map_schema.borrow_mut() {
            map_schema.add_key(name, schema)
        } else {
            Err(ParserError::SchemaError(
                "Source schema is not defined".into(),
            ))
        }
    }

    fn remove_source_key(&self, name: &str) {
        if let Some(ref mut map_schema) = *self.source_map_schema.borrow_mut() {
            map_schema.remove_key(name);
        }
    }

    fn rename_source_key(&self, old_name: &str, new_name: &str) {
        if let Some(ref mut map_schema) = *self.source_map_schema.borrow_mut() {
            map_schema.rename_key(old_name, new_name);
        }
    }

    fn add_summary_key(&self, name: &str, schema: crate::ParserMapKeySchema) -> Result<(), ParserError> {
        if let Some(ref mut map_schema) = *self.summary_map_schema.borrow_mut() {
            map_schema.add_key(name, schema)
        } else {
            Err(ParserError::SchemaError(
                "Summary schema is not defined".into(),
            ))
        }
    }

    fn remove_summary_key(&self, name: &str) {
        if let Some(ref mut map_schema) = *self.summary_map_schema.borrow_mut() {
            map_schema.remove_key(name);
        }
    }

    fn rename_summary_key(&self, old_name: &str, new_name: &str) {
        if let Some(ref mut map_schema) = *self.summary_map_schema.borrow_mut() {
            map_schema.rename_key(old_name, new_name);
        }
    }
}

pub struct ParserStateScope<'a> {
    pipeline_builder: &'a RefCell<PipelineExpressionBuilder>,
    source_map_schema: RefCell<Option<ParserMapSchema>>,
    summary_map_schema: RefCell<Option<ParserMapSchema>>,
    attached_data_names: HashSet<Box<str>>,
    global_variable_names: &'a RefCell<HashSet<Box<str>>>,
    variable_names: RefCell<HashSet<Box<str>>>,
    constants: &'a RefCell<HashMap<Box<str>, (usize, ValueType)>>,
}

impl ParserScope for ParserStateScope<'_> {
    fn get_pipeline(&self) -> Ref<'_, PipelineExpression> {
        Ref::map(self.pipeline_builder.borrow(), |v| v.as_ref())
    }

    fn get_source_schema(&self) -> Option<Ref<'_, ParserMapSchema>> {
        let borrow = self.source_map_schema.borrow();
        if borrow.is_some() {
            Some(Ref::map(borrow, |opt| opt.as_ref().unwrap()))
        } else {
            None
        }
    }

    fn get_summary_schema(&self) -> Option<Ref<'_, ParserMapSchema>> {
        let borrow = self.summary_map_schema.borrow();
        if borrow.is_some() {
            Some(Ref::map(borrow, |opt| opt.as_ref().unwrap()))
        } else {
            None
        }
    }

    fn is_well_defined_identifier(&self, name: &str) -> bool {
        name == "source"
            || self.is_attached_data_defined(name)
            || self.is_variable_defined(name)
            || self.get_constant_id(name).is_some()
    }

    fn is_attached_data_defined(&self, name: &str) -> bool {
        self.attached_data_names.contains(name)
    }

    fn is_variable_defined(&self, name: &str) -> bool {
        self.variable_names.borrow().contains(name)
            || self.global_variable_names.borrow().contains(name)
    }

    fn get_constant_id(&self, name: &str) -> Option<(usize, ValueType)> {
        self.constants.borrow().get(name).cloned()
    }

    fn get_constant_name(&self, id: usize) -> Option<(Box<str>, ValueType)> {
        self.constants
            .borrow()
            .iter()
            .find(|(_, v)| v.0 == id)
            .map(|(k, v)| (k.clone(), v.1.clone()))
    }

    fn push_variable_name(&self, name: &str) {
        self.variable_names.borrow_mut().insert(name.into());
    }

    fn push_constant(&self, name: &str, value: StaticScalarExpression) -> usize {
        let value_type = value.get_value_type();
        let constant_id = self.pipeline_builder.borrow_mut().push_constant(value);

        self.constants
            .borrow_mut()
            .insert(name.into(), (constant_id, value_type));

        constant_id
    }

    fn create_scope<'a>(&'a self, options: ParserOptions) -> ParserStateScope<'a> {
        ParserStateScope {
            pipeline_builder: self.pipeline_builder,
            source_map_schema: RefCell::new(options.source_map_schema),
            summary_map_schema: RefCell::new(options.summary_map_schema),
            attached_data_names: options.attached_data_names,
            global_variable_names: self.global_variable_names,
            variable_names: RefCell::new(HashSet::new()),
            constants: self.constants,
        }
    }

    fn add_source_key(&self, name: &str, schema: crate::ParserMapKeySchema) -> Result<(), ParserError> {
        if let Some(ref mut map_schema) = *self.source_map_schema.borrow_mut() {
            map_schema.add_key(name, schema)
        } else {
            Err(ParserError::SchemaError(
                "Source schema is not defined".into(),
            ))
        }
    }

    fn remove_source_key(&self, name: &str) {
        if let Some(ref mut map_schema) = *self.source_map_schema.borrow_mut() {
            map_schema.remove_key(name);
        }
    }

    fn rename_source_key(&self, old_name: &str, new_name: &str) {
        if let Some(ref mut map_schema) = *self.source_map_schema.borrow_mut() {
            map_schema.rename_key(old_name, new_name);
        }
    }

    fn add_summary_key(&self, name: &str, schema: crate::ParserMapKeySchema) -> Result<(), ParserError> {
        if let Some(ref mut map_schema) = *self.summary_map_schema.borrow_mut() {
            map_schema.add_key(name, schema)
        } else {
            Err(ParserError::SchemaError(
                "Summary schema is not defined".into(),
            ))
        }
    }

    fn remove_summary_key(&self, name: &str) {
        if let Some(ref mut map_schema) = *self.summary_map_schema.borrow_mut() {
            map_schema.remove_key(name);
        }
    }

    fn rename_summary_key(&self, old_name: &str, new_name: &str) {
        if let Some(ref mut map_schema) = *self.summary_map_schema.borrow_mut() {
            map_schema.rename_key(old_name, new_name);
        }
    }
}

pub trait ParserScope {
    fn get_pipeline(&self) -> Ref<'_, PipelineExpression>;

    fn get_query(&self) -> Ref<'_, str> {
        Ref::map(self.get_pipeline(), |p| p.get_query())
    }

    fn get_query_slice(&self, query_location: &QueryLocation) -> Ref<'_, str> {
        Ref::map(self.get_pipeline(), |p| p.get_query_slice(query_location))
    }

    fn get_source_schema(&self) -> Option<Ref<'_, ParserMapSchema>>;

    fn get_summary_schema(&self) -> Option<Ref<'_, ParserMapSchema>>;

    fn is_well_defined_identifier(&self, name: &str) -> bool;

    fn is_attached_data_defined(&self, name: &str) -> bool;

    fn is_variable_defined(&self, name: &str) -> bool;

    fn get_constant_id(&self, name: &str) -> Option<(usize, ValueType)>;

    fn get_constant_name(&self, id: usize) -> Option<(Box<str>, ValueType)>;

    fn push_variable_name(&self, name: &str);

    fn push_constant(&self, name: &str, value: StaticScalarExpression) -> usize;

    fn create_scope<'a>(&'a self, options: ParserOptions) -> ParserStateScope<'a>;

    fn add_source_key(&self, _name: &str, _schema: crate::ParserMapKeySchema) -> Result<(), ParserError> {
        Err(ParserError::SchemaError(
            "Cannot add source key in this scope".into(),
        ))
    }

    fn remove_source_key(&self, _name: &str) {
        // Default implementation does nothing
    }

    fn rename_source_key(&self, _old_name: &str, _new_name: &str) {
        // Default implementation does nothing
    }

    fn add_summary_key(&self, _name: &str, _schema: crate::ParserMapKeySchema) -> Result<(), ParserError> {
        Err(ParserError::SchemaError(
            "Cannot add summary key in this scope".into(),
        ))
    }

    fn remove_summary_key(&self, _name: &str) {
        // Default implementation does nothing
    }

    fn rename_summary_key(&self, _old_name: &str, _new_name: &str) {
        // Default implementation does nothing
    }

    fn try_resolve_value_type(
        &self,
        scalar: &mut ScalarExpression,
    ) -> Result<Option<ValueType>, ParserError> {
        scalar
            .try_resolve_value_type(&self.get_pipeline().get_resolution_scope())
            .map_err(|e| ParserError::from(&e))
    }
}
