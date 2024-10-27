use super::{condition::Condition, recursive_parser::parse_condition};
use crate::{errors::CQLError, logical_operator::LogicalOperator, operator::Operator};

/// Struct representing the `IF` CQL clause.
///
/// The `IF` clause is used to add a condition to a query, which must be met in order to execute the query.
///
/// # Fields
///
/// * `condition` - The condition to be evaluated.
///
#[derive(Debug, PartialEq, Clone)]
pub struct If {
    pub condition: Condition,
}

impl If {
    /// Creates and returns a new `If` instance from a vector of tokens.
    ///
    /// # Arguments
    ///
    /// * `tokens` - A vector of tokens that can be used to build a `If` instance.
    ///
    /// The tokens should be in the following order: `IF`, `column`, `operator`, `value` in the case of a simple condition, and `IF`, `condition`, `AND`, `condition` for a complex condition.
    ///
    /// # Examples
    ///
    /// ```
    /// let tokens = vec!["IF", "age", "=", "18"];
    /// let if_from_tokens = If::new_from_tokens(tokens).unwrap();
    /// let if_clause = If {
    ///    condition: Condition::Simple {
    ///         column: "age".to_string(),
    ///         operator: Operator::Equal,
    ///         value: "18".to_string(),
    ///     },
    /// };
    ///
    /// assert_eq!(if_from_tokens, if_clause);
    /// ```
    ///
    pub fn new_from_tokens(tokens: Vec<&str>) -> Result<Self, CQLError> {
        if tokens.len() < 4 {
            return Err(CQLError::InvalidSyntax);
        }
        let mut pos = 1;
        let condition = parse_condition(&tokens, &mut pos)?;

        Ok(Self { condition })
    }

    pub fn serialize(&self) -> String {
        self.condition.serialize()
    }

    /// Validates that none of the conditions in the `IF` clause are related to partition or clustering keys.
    ///
    /// # Arguments
    ///
    /// * `partitioner_keys` - Vector with the names of the primary keys.
    /// * `clustering_columns` - Vector with the names of the clustering columns.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the conditions meet the requirements.
    /// * `Err(CQLError::InvalidCondition)` if any of the validations fail.
    pub fn validate_cql_conditions(
        &self,
        partitioner_keys: &Vec<String>,
        clustering_columns: &Vec<String>,
    ) -> Result<(), CQLError> {
        self.recursive_validate_no_partition_clustering(
            &self.condition,
            partitioner_keys,
            clustering_columns,
        )
    }

    /// Recursive method to validate that conditions do not include partition or clustering keys.
    fn recursive_validate_no_partition_clustering(
        &self,
        condition: &Condition,
        partitioner_keys: &Vec<String>,
        clustering_columns: &Vec<String>,
    ) -> Result<(), CQLError> {
        match condition {
            Condition::Simple { field, .. } => {
                // Check if the field is a partition or clustering key
                if partitioner_keys.contains(field) || clustering_columns.contains(field) {
                    return Err(CQLError::InvalidCondition);
                }
            }
            Condition::Complex { left, right, .. } => {
                // Validate recursively for both left and right conditions
                if let Some(left_condition) = left.as_ref() {
                    self.recursive_validate_no_partition_clustering(
                        left_condition,
                        partitioner_keys,
                        clustering_columns,
                    )?;
                }
                self.recursive_validate_no_partition_clustering(
                    right,
                    partitioner_keys,
                    clustering_columns,
                )?;
            }
        }
        Ok(())
    }

    /// Returns the values of the primary keys in the conditions of the `If` clause.
    pub fn get_value_partitioner_key_condition(
        &self,
        partitioner_keys: Vec<String>,
    ) -> Result<Vec<String>, CQLError> {
        let mut result = vec![];

        match &self.condition {
            Condition::Simple {
                field,
                operator,
                value,
            } => {
                // If it is a simple condition and the key is in partitioner_keys and the operator is `=`
                if partitioner_keys.contains(field) && *operator == Operator::Equal {
                    result.push(value.clone());
                }
            }
            Condition::Complex { left, .. } => {
                // Traverse the left condition
                if let Some(left_condition) = left.as_ref() {
                    self.collect_partitioner_key_values(
                        left_condition,
                        &partitioner_keys,
                        &mut result,
                    );
                }
            }
        }

        if result.is_empty() {
            Err(CQLError::InvalidColumn)
        } else {
            Ok(result)
        }
    }

    /// Helper method to traverse conditions and collect values of partitioner keys.
    fn collect_partitioner_key_values(
        &self,
        condition: &Condition,
        partitioner_keys: &[String],
        result: &mut Vec<String>,
    ) {
        match condition {
            Condition::Simple {
                field,
                operator,
                value,
            } => {
                // If the simple condition corresponds to a partitioner key
                if partitioner_keys.contains(field) && *operator == Operator::Equal {
                    result.push(value.clone());
                }
            }
            Condition::Complex {
                left,
                operator,
                right,
            } => {
                // Only process if it is a logical AND operator
                if *operator == LogicalOperator::And {
                    if let Some(left_condition) = left.as_ref() {
                        self.collect_partitioner_key_values(
                            left_condition,
                            partitioner_keys,
                            result,
                        );
                    }
                    self.collect_partitioner_key_values(right, partitioner_keys, result);
                }
            }
        }
    }
}
