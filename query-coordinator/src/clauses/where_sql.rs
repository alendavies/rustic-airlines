use super::{condition::Condition, recursive_parser::parse_condition};
use crate::{errors::CQLError, logical_operator::LogicalOperator, operator::Operator};

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
    /// Valida que las condiciones sean `primary_key = algo` primero y que las condiciones posteriores
    /// solo sean `AND` relacionados con las `clustering_columns`.
    ///
    /// # Arguments
    ///
    /// * `partitioner_keys` - Vector con los nombres de las claves primarias que deben aparecer en las primeras condiciones.
    /// * `clustering_columns` - Vector con los nombres de las columnas de clustering que deben aparecer en las condiciones posteriores.
    ///
    /// # Returns
    ///
    /// * `Ok(())` si las condiciones cumplen con los requisitos.
    /// * `Err(CQLError::InvalidCondition)` si no se cumple alguna de las validaciones.
    /// Valida que las condiciones sean `partition_key = algo` primero, y luego comparaciones con `clustering_columns`.
    ///
    /// # Arguments
    ///
    /// * `partitioner_keys` - Vector con los nombres de las claves primarias.
    /// * `clustering_columns` - Vector con los nombres de las columnas de clustering.
    ///
    /// # Returns
    ///
    /// * `Ok(())` si las condiciones cumplen con los requisitos.
    /// * `Err(CQLError::InvalidCondition)` si no se cumple alguna de las validaciones.
    /// Valida que las condiciones sean correctas para una operación de `DELETE` o `UPDATE`.
    ///
    /// # Arguments
    ///
    /// * `partitioner_keys` - Vector con los nombres de las claves primarias.
    /// * `clustering_columns` - Vector con los nombres de las columnas de clustering.
    /// * `delete` - Booleano que indica si es una operación de DELETE.
    /// * `update` - Booleano que indica si es una operación de UPDATE.
    ///
    /// # Returns
    ///
    /// * `Ok(())` si las condiciones cumplen con los requisitos.
    /// * `Err(CQLError::InvalidCondition)` si no se cumple alguna de las validaciones.
    pub fn validate_cql_conditions(
        &self,
        partitioner_keys: &Vec<String>,
        clustering_columns: &Vec<String>,
        delete: bool,
        update: bool,
    ) -> Result<(), CQLError> {
        let mut partitioner_key_count = 0;
        let mut partitioner_keys_verified = false;
        let mut clustering_key_count = 0;

        // Valida recursivamente las condiciones
        self.recursive_validate_conditions(
            &self.condition,
            partitioner_keys,
            clustering_columns,
            &mut partitioner_key_count,
            &mut partitioner_keys_verified,
            &mut clustering_key_count,
            delete,
            update,
        )?;

        // En caso de `UPDATE`, verificar que todas las clustering columns hayan sido comparadas
        if update && clustering_key_count != clustering_columns.len() {
            return Err(CQLError::InvalidCondition); // No se han comparado todas las clustering columns
        }
        Ok(())
    }

    /// Método recursivo para validar las condiciones de las claves primarias y de clustering.
    fn recursive_validate_conditions(
        &self,
        condition: &Condition,
        partitioner_keys: &Vec<String>,
        clustering_columns: &Vec<String>,
        partitioner_key_count: &mut usize,
        partitioner_keys_verified: &mut bool,
        clustering_key_count: &mut usize,
        delete_or_select: bool,
        update: bool,
    ) -> Result<(), CQLError> {
        match condition {
            Condition::Simple {
                field, operator, ..
            } => {
                // Si no hemos verificado todas las partitioner keys, verificamos solo claves primarias con `=`
                if !*partitioner_keys_verified {
                    if partitioner_keys.contains(field) && *operator == Operator::Equal {
                        *partitioner_key_count += 1;
                        if *partitioner_key_count == partitioner_keys.len() {
                            *partitioner_keys_verified = true; // Todas las claves primarias han sido verificadas
                        }
                    } else {
                        return Err(CQLError::InvalidCondition); // La clave no es de partición o el operador no es `=`
                    }
                } else {
                    // Si ya verificamos las partitioner keys, ahora validamos clustering columns
                    if !clustering_columns.contains(field) {
                        return Err(CQLError::InvalidCondition); // No es una clustering column válida
                    }
                    // En caso de `UPDATE`, verificamos que todas las clustering columns se comparen
                    if update {
                        if *operator != Operator::Equal {
                            return Err(CQLError::InvalidCondition); // Las clustering columns deben compararse con `=`
                        }
                        *clustering_key_count += 1;
                    }
                }
            }
            Condition::Complex {
                left,
                operator,
                right,
            } => {
                // Verificar que el operador sea `AND` si aún estamos verificando partitioner keys
                if !*partitioner_keys_verified && *operator != LogicalOperator::And {
                    return Err(CQLError::InvalidCondition); // Solo se permite `AND` para las partitioner keys
                }

                // Si es un `UPDATE`, después de verificar las partitioner keys, solo permitimos `AND` para clustering columns
                if update && *partitioner_keys_verified && *operator != LogicalOperator::And {
                    return Err(CQLError::InvalidCondition); // Solo se permite `AND` para las clustering columns en `UPDATE`
                }

                // Verificación recursiva en las condiciones anidadas
                if let Some(left_condition) = left.as_ref() {
                    self.recursive_validate_conditions(
                        &*left_condition,
                        partitioner_keys,
                        clustering_columns,
                        partitioner_key_count,
                        partitioner_keys_verified,
                        clustering_key_count,
                        delete_or_select,
                        update,
                    )?;
                }

                self.recursive_validate_conditions(
                    right,
                    partitioner_keys,
                    clustering_columns,
                    partitioner_key_count,
                    partitioner_keys_verified,
                    clustering_key_count,
                    delete_or_select,
                    update,
                )?;
            }
        }

        Ok(())
    }

    /// Retorna los valores de las claves primarias de las condiciones en la cláusula `WHERE`.
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
                // Si es una condición simple y la clave está en partitioner_keys y el operador es `=`
                if partitioner_keys.contains(field) && *operator == Operator::Equal {
                    result.push(value.clone());
                }
            }
            Condition::Complex { left, .. } => {
                // Recorremos la condición izquierda
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

    /// Método auxiliar para recorrer las condiciones y recolectar los valores de las partitioner keys.
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
                // Si la condición simple corresponde a una partitioner key
                if partitioner_keys.contains(field) && *operator == Operator::Equal {
                    result.push(value.clone());
                }
            }
            Condition::Complex {
                left,
                operator,
                right,
            } => {
                // Solo procesar si es un operador lógico AND
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
