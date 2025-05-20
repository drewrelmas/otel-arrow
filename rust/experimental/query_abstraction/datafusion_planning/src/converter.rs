use datafusion::prelude::*;
use datafusion::logical_expr::{col, lit, LogicalPlan};
use intermediate_language::grammar_objects::*;

pub fn logical_plan_from_query(df: DataFrame, query: &Query) -> datafusion::error::Result<LogicalPlan> {
    let mut df = df;

    for statement in &query.statements {
        match statement {
            Statement::Filter(predicate) => {
                let expr = predicate_to_expr(predicate);
                df = df.filter(expr)?;
            }
            Statement::Extend(alias, expression, _predicate) => {
                let expr = expression_to_expr(expression);
                df = df.with_column(&alias.name, expr)?;
            }
        }
    }

    Ok(df.logical_plan().clone())
}

fn predicate_to_expr(predicate: &Predicate) -> datafusion::logical_expr::Expr {
    match predicate {
        Predicate::BinaryLogicalExpression(expr) => {
            let left = expression_to_expr(&expr.left);
            let right = expression_to_expr(&expr.right);
            match expr.boolean_operator {
                BooleanOperator::And => left.and(right),
                BooleanOperator::Or => left.or(right),
            }
        }
        Predicate::ComparisonExpression(expr) => {
            let left = expression_to_expr(&expr.left);
            let right = expression_to_expr(&expr.right);
            match expr.comparison_operator {
                ComparisonOperator::Equal => left.eq(right),
                ComparisonOperator::NotEqual => left.not_eq(right),
                ComparisonOperator::GreaterThan => left.gt(right),
                ComparisonOperator::LessThan => left.lt(right),
                ComparisonOperator::GreaterThanOrEqual => left.gt_eq(right),
                ComparisonOperator::LessThanOrEqual => left.lt_eq(right),
            }
        }
        Predicate::NegatedExpression(expr) => expression_to_expr(expr).not(),
    }
}

fn expression_to_expr(expression: &Expression) -> datafusion::logical_expr::Expr {
    match expression {
        Expression::Identifier(identifier) => col(&identifier.name),
        Expression::Literal(literal) => match literal {
            Literal::Bool(value) => lit(*value),
            Literal::Int(value) => lit(*value),
            Literal::String(value) => lit(value.clone()),
        },
        Expression::Predicate(predicate) => predicate_to_expr(predicate),
        Expression::EnclosedExpression(expr) => expression_to_expr(expr),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::{array::{
        ArrayRef, BooleanArray, RecordBatch, StringArray, UInt16Array, UInt8Array
    }, datatypes::Schema};
    

    fn create_sample_record_batch() -> RecordBatch {
        let book_id = UInt8Array::from(vec![1, 2, 3, 4, 5]);
        let title = StringArray::from(vec![
            "The Rust Book",
            "Programming in Arrow",
            "Data Fusion 101",
            "Query Engines Explained",
            "Advanced Rust Patterns",
        ]);
        let author = StringArray::from(vec![
            "Alice",
            "Bob",
            "Carol",
            "Dave",
            "Eve",
        ]);
        let published_year = UInt16Array::from(vec![
            Some(1950u16),
            Some(2020u16),
            None,
            Some(2019u16),
            Some(2021u16),
        ]);
        let available = BooleanArray::from(vec![true, false, true, true, false]);

        let columns: Vec<ArrayRef> = vec![
            Arc::new(book_id),
            Arc::new(title),
            Arc::new(author),
            Arc::new(published_year),
            Arc::new(available),
        ];

        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("book_id", arrow::datatypes::DataType::UInt8, false),
            arrow::datatypes::Field::new("title", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("author", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("published_year", arrow::datatypes::DataType::UInt16, true),
            arrow::datatypes::Field::new("available", arrow::datatypes::DataType::Boolean, false),
        ]));
        RecordBatch::try_new(schema, columns).unwrap()
    }

    #[test]
    fn test_create_sample_record_batch() {
        let batch = create_sample_record_batch();
        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 5);
    }

    #[tokio::test]
    async fn test_simple_query() {
        let batch = create_sample_record_batch();
        let query = Query {
            source: "classics".to_string(),
            statements: vec![
                Statement::Filter(Predicate::ComparisonExpression(ComparisonExpression {
                    left: Box::new(Expression::Identifier(Identifier { name: "published_year".to_string() })),
                    right: Box::new(Expression::Literal(Literal::Int(2000))),
                    comparison_operator: ComparisonOperator::LessThan,
                })),
                Statement::Extend(
                    Identifier { name: "is_classic".to_string() },
                    Expression::Literal(Literal::Bool(true)),
                    None,
                ),
            ],
        };

        let ctx = SessionContext::new();
        let df = ctx.read_batch(batch.clone()).unwrap();

        let logical_plan = logical_plan_from_query(df, &query).unwrap();
        assert_eq!(logical_plan.schema().fields().len(), 6);

        ctx.register_batch(&query.source, batch.clone()).unwrap();
        let df = DataFrame::new(ctx.state(), logical_plan);
        let results = df.collect().await.unwrap();

        assert_eq!(results.len(), 1);
        let result_batch = &results[0];
        assert_eq!(result_batch.num_rows(), 1);
        assert_eq!(result_batch.num_columns(), 6);
        assert_eq!(result_batch.column(5).data_type(), &arrow::datatypes::DataType::Boolean);
        let is_classic_array = result_batch.column(5).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(is_classic_array.value(0), true);
    }
}