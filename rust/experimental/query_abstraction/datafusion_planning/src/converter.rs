use datafusion::prelude::*;
use datafusion::logical_expr::{col, lit, LogicalPlan};
use intermediate_language::grammar_objects::*;

pub async fn convert_query_to_datafusion_plan(query: Query) -> Result<LogicalPlan, datafusion::error::DataFusionError> {
    let ctx = SessionContext::new();

    let mut plan = ctx.read_csv("test.csv", CsvReadOptions::new()).await?;

    for statement in query.statements {
        plan = match statement {
            Statement::Filter(predicate) => {
                let filter_expr = convert_predicate_to_expr(predicate)?;
                plan.filter(filter_expr)?
            }
            Statement::Extend(identifier, expression, predicate_opt) => {
                let expr = convert_expression_to_expr(expression)?;
                let column_name = identifier.name;

                if let Some(predicate) = predicate_opt {
                    let condition = convert_predicate_to_expr(predicate)?;
                    plan = plan.with_column(&column_name, expr)?;
                    plan.filter(condition)?
                } else {
                    plan.with_column(&column_name, expr)?
                }
            }
        };
    }

    Ok(plan.logical_plan().clone())
}

fn convert_predicate_to_expr(predicate: Predicate) -> Result<Expr, datafusion::error::DataFusionError> {
    match predicate {
        Predicate::BinaryLogicalExpression(binary_expr) => {
            let left = convert_expression_to_expr(*binary_expr.left)?;
            let right = convert_expression_to_expr(*binary_expr.right)?;
            match binary_expr.boolean_operator {
                BooleanOperator::And => Ok(left.and(right)),
                BooleanOperator::Or => Ok(left.or(right)),
            }
        }
        Predicate::ComparisonExpression(comp_expr) => {
            let left = convert_expression_to_expr(*comp_expr.left)?;
            let right = convert_expression_to_expr(*comp_expr.right)?;
            match comp_expr.comparison_operator {
                ComparisonOperator::Equal => Ok(left.eq(right)),
                ComparisonOperator::NotEqual => Ok(left.not_eq(right)),
                ComparisonOperator::GreaterThan => Ok(left.gt(right)),
                ComparisonOperator::LessThan => Ok(left.lt(right)),
                ComparisonOperator::GreaterThanOrEqual => Ok(left.gt_eq(right)),
                ComparisonOperator::LessThanOrEqual => Ok(left.lt_eq(right)),
            }
        }
        Predicate::NegatedExpression(expr) => {
            let inner_expr = convert_expression_to_expr(*expr)?;
            Ok(inner_expr.not())
        }
    }
}

fn convert_expression_to_expr(expression: Expression) -> Result<Expr, datafusion::error::DataFusionError> {
    match expression {
        Expression::Identifier(identifier) => Ok(col(&identifier.name)),
        Expression::Literal(literal) => match literal {
            Literal::Bool(value) => Ok(lit(value)),
            Literal::Int(value) => Ok(lit(value)),
            Literal::String(value) => Ok(lit(value)),
        },
        Expression::Predicate(predicate) => convert_predicate_to_expr(predicate),
        Expression::EnclosedExpression(inner_expr) => convert_expression_to_expr(*inner_expr),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use intermediate_language::grammar_objects::*;

    #[tokio::test]
    async fn test_simple_filter_plan() {
        let query = Query {
            source: "test.csv".to_string(),
            statements: vec![
                Statement::Filter(Predicate::ComparisonExpression(ComparisonExpression {
                    left: Box::new(Expression::Identifier(Identifier { name: "age".to_string() })),
                    right: Box::new(Expression::Literal(Literal::Int(30))),
                    comparison_operator: ComparisonOperator::GreaterThan,
                })),
            ],
        };

        let plan = convert_query_to_datafusion_plan(query).await;
        assert!(plan.is_ok());
    }

    #[tokio::test]
    async fn test_extend_with_predicate() {
        let query = Query {
            source: "test.csv".to_string(),
            statements: vec![
                Statement::Extend(
                    Identifier { name: "is_adult".to_string() },
                    Expression::Predicate(Predicate::ComparisonExpression(ComparisonExpression {
                        left: Box::new(Expression::Identifier(Identifier { name: "age".to_string() })),
                        right: Box::new(Expression::Literal(Literal::Int(18))),
                        comparison_operator: ComparisonOperator::GreaterThanOrEqual,
                    })),
                    Some(Predicate::ComparisonExpression(ComparisonExpression {
                        left: Box::new(Expression::Identifier(Identifier { name: "country".to_string() })),
                        right: Box::new(Expression::Literal(Literal::String("US".to_string()))),
                        comparison_operator: ComparisonOperator::Equal,
                    })),
                ),
            ],
        };

        let plan = convert_query_to_datafusion_plan(query).await;
        assert!(plan.is_ok());
    }

    #[tokio::test]
    async fn test_filter_with_and_or() {
        let query = Query {
            source: "test.csv".to_string(),
            statements: vec![
                Statement::Filter(Predicate::BinaryLogicalExpression(BinaryLogicalExpression {
                    left: Box::new(Expression::Predicate(Predicate::ComparisonExpression(ComparisonExpression {
                        left: Box::new(Expression::Identifier(Identifier { name: "score".to_string() })),
                        right: Box::new(Expression::Literal(Literal::Int(50))),
                        comparison_operator: ComparisonOperator::GreaterThan,
                    }))),
                    right: Box::new(Expression::Predicate(Predicate::ComparisonExpression(ComparisonExpression {
                        left: Box::new(Expression::Identifier(Identifier { name: "passed".to_string() })),
                        right: Box::new(Expression::Literal(Literal::Bool(true))),
                        comparison_operator: ComparisonOperator::Equal,
                    }))),
                    boolean_operator: BooleanOperator::And,
                })),
            ],
        };

        let plan = convert_query_to_datafusion_plan(query).await;
        assert!(plan.is_ok());
    }

    #[test]
    fn test_convert_predicate_to_expr_not() {
        let pred = Predicate::NegatedExpression(Box::new(Expression::Predicate(
            Predicate::ComparisonExpression(ComparisonExpression {
                left: Box::new(Expression::Identifier(Identifier { name: "flag".to_string() })),
                right: Box::new(Expression::Literal(Literal::Bool(true))),
                comparison_operator: ComparisonOperator::Equal,
            }),
        )));
        let expr = convert_predicate_to_expr(pred);
        assert!(expr.is_ok());
        let expr = expr.unwrap();
        match expr {
            Expr::Not(_) => {}
            _ => panic!("Expected Not expression"),
        }
    }

    #[test]
    fn test_convert_expression_to_expr_literal() {
        let expr = convert_expression_to_expr(Expression::Literal(Literal::Int(42)));
        assert!(expr.is_ok());
        let expr = expr.unwrap();
        match expr {
            Expr::Literal(_) => {}
            _ => panic!("Expected Literal expression"),
        }
    }
}
