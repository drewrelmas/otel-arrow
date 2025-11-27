// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use data_engine_expressions::*;
use data_engine_parser_abstractions::*;
use pest_derive::Parser;

use crate::query_expression::parse_query;

#[derive(Parser)]
#[grammar = "kql.pest"]
pub(crate) struct KqlPestParser;

pub struct KqlParser {}

impl Parser for KqlParser {
    fn parse_with_options(
        query: &str,
        options: ParserOptions,
    ) -> Result<PipelineExpression, Vec<ParserError>> {
        parse_query(query, options)
    }
}

impl KqlParser {
    /// Parse with options and return the result along with the mutated schemas.
    /// This is useful for testing to inspect how schemas were modified during parsing.
    /// 
    /// Returns a tuple of (parse_result, source_schema, summary_schema).
    pub fn parse_for_schema(
        query: &str,
        options: ParserOptions,
    ) -> (Result<PipelineExpression, Vec<ParserError>>, Option<ParserMapSchema>, Option<ParserMapSchema>) {
        let state = ParserState::new_with_options(query, options);
        
        // Parse and populate the state
        let parse_result = crate::query_expression::parse_query_internal(query, &state);
        
        // Extract schemas before consuming the state
        let source_schema = state.extract_source_schema();
        let summary_schema = state.extract_summary_schema();
        
        // Build the final result if parsing succeeded
        let result = match parse_result {
            Ok(()) => state.build(),
            Err(errors) => Err(errors),
        };
        
        (result, source_schema, summary_schema)
    }
}

pub(crate) fn map_kql_errors(error: ParserError) -> ParserError {
    match error {
        ParserError::KeyNotFound { location, key } => ParserError::QueryLanguageDiagnostic {
            location,
            diagnostic_id: "KS142",
            message: format!(
                "The name '{key}' does not refer to any known column, table, variable or function"
            ),
        },
        e => e,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_parse() {
        assert!(KqlParser::parse("a").is_ok());
        assert!(KqlParser::parse("let a = 1").is_err());
        assert!(KqlParser::parse("i | extend a = 1 i | extend b = 2").is_err());
    }
}
