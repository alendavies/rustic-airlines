use super::{condition::Condition, recursive_parser::parse_condition};
use crate::{
    errors::CQLError, logical_operator::LogicalOperator, operator::Operator, 
};

/// Struct representing the `WHERE` SQL clause.
///
/// The `WHERE` clause is used to filter records that match a certain condition.
///
/// # Fields
///
/// * `condition` - The condition to be evaluated.
///
#[derive(Debug, PartialEq, Clone)]
pub struct Where {
    pub condition: Condition,
}

impl Where {
    /// Creates and returns a new `Where` instance from a vector of tokens.
    ///
    /// # Arguments
    ///
    /// * `tokens` - A vector of tokens that can be used to build a `Where` instance.
    ///
    /// The tokens should be in the following order: `WHERE`, `column`, `operator`, `value` in the case of a simple condition, and `WHERE`, `condition`, `AND` or `OR`, `condition` for a complex condition.
    ///
    /// # Examples
    ///
    /// ```
    /// let tokens = vec!["WHERE", "age", ">", "18"];
    /// let where_from_tokens = Where::new_from_tokens(tokens).unwrap();
    /// let where_clause = Where {
    ///    condition: Condition::Simple {
    ///         column: "age".to_string(),
    ///         operator: Operator::Greater,
    ///         value: "18".to_string(),
    ///     },
    /// };
    ///
    /// assert_eq!(where_from_tokens, where_clause);
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

     /// Valida que la primera condición sea `primary_key = algo` y que las condiciones posteriores
    /// solo sean `AND` anidados relacionados con la `clustering_column`.
    ///
    /// # Arguments
    ///
    /// * `primary_key` - El nombre de la clave primaria que debe aparecer en la primera condición.
    /// * `clustering_column` - El nombre de la columna de clustering que debe aparecer en las condiciones posteriores.
    ///
    /// # Returns
    ///
    /// * `Ok(())` si las condiciones cumplen con los requisitos.
    /// * `Err(CQLError::InvalidCondition)` si no se cumple alguna de las validaciones.
    pub fn validate_cql_conditions(&self, primary_key: &str, clustering_column: &str) -> Result<(), CQLError> {
        match &self.condition {
            Condition::Simple { field, operator, .. } => {
                // Verifica que la primera condición sea `primary_key = algo`
                if field != primary_key || *operator != Operator::Equal {
                    return Err(CQLError::InvalidSyntax);
                }
            }
            Condition::Complex { left, operator, right } => {
                // Verifica que la primera condición compleja sea `primary_key = algo`
                if let Some(left_condition) = left.as_ref() {
                    if let Condition::Simple { field, operator, .. } = &**left_condition {
                        if field != primary_key || *operator != Operator::Equal {
                            return Err(CQLError::InvalidSyntax);
                        }
                    } else {
                        return Err(CQLError::InvalidSyntax);
                    }
                } else {
                    return Err(CQLError::InvalidSyntax);
                }
    
                // Verifica que el operador lógico sea `AND`
                if *operator != LogicalOperator::And {
                    return Err(CQLError::InvalidSyntax);
                }
    
                // Verifica que todas las condiciones derechas se refieran a la clustering column
                self.check_clustering_conditions(right, clustering_column)?;
            }
        }
    
        Ok(())
    }
    

    /// Método auxiliar que verifica que todas las condiciones en una estructura compleja estén relacionadas
    /// con la `clustering_column` usando solo `AND`.
    fn check_clustering_conditions(&self, condition: &Condition, clustering_column: &str) -> Result<(), CQLError> {
        match condition {
            Condition::Simple { field, .. } => {
                // Verifica que el campo sea la clustering_column
                if field != clustering_column {
                    return Err(CQLError::InvalidSyntax);
                }
            }
            Condition::Complex { left, operator, right } => {
                // Solo se permite el operador `AND`
                if *operator != LogicalOperator::And {
                    return Err(CQLError::InvalidSyntax);
                }

                // Verifica recursivamente en las condiciones anidadas
                if let Some(left_condition) = left.as_ref() {
                    self.check_clustering_conditions(left_condition, clustering_column)?;
                }
                self.check_clustering_conditions(right, clustering_column)?;
            }
        }
        Ok(())
    }

    pub fn get_value_primary_condition(&self, primary_key: &str) -> Result<Option<String>, CQLError> {
        match &self.condition {
            Condition::Simple { field, operator, value } => {
                // Verifica si la condición es `primary_key = algo`
                if field == primary_key && *operator == Operator::Equal {
                    return Ok(Some(value.clone()));
                }
            }
            Condition::Complex { left, .. } => {
                // Verifica si la primera condición en la parte izquierda es `primary_key = algo`
                if let Some(left_condition) = left.as_ref() {
                    if let Condition::Simple { field, operator, value } = &**left_condition {
                        if field == primary_key && *operator == Operator::Equal {
                            return Ok(Some(value.clone()));
                        }
                    }
                }
            }
        }

        // Retorna None si no se cumple `primary_key = algo` en la primera condición
        Ok(None)
    }


}
