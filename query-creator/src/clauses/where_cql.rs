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
        delete_or_select: bool,
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
            delete_or_select,
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
            Condition::Complex { left, right, .. } => {
                // Recorremos la condición izquierda
                if let Some(left_condition) = left.as_ref() {
                    self.collect_partitioner_key_values(
                        left_condition,
                        &partitioner_keys,
                        &mut result,
                    );
                }
                self.collect_partitioner_key_values(&right, &partitioner_keys, &mut result);
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

    /// Retorna los valores de las clustering columns de las condiciones en la cláusula `WHERE`.
    pub fn get_value_clustering_column_condition(
        &self,
        clustering_columns: Vec<String>,
    ) -> Option<Vec<String>> {
        let mut result = vec![];

        match &self.condition {
            Condition::Simple {
                field,
                operator,
                value,
            } => {
                // Si es una condición simple y la clave está en clustering_columns y el operador es `=`
                if clustering_columns.contains(field) && *operator == Operator::Equal {
                    result.push(value.clone());
                }
            }
            Condition::Complex { left, right, .. } => {
                // Recorremos la condición izquierda
                if let Some(left_condition) = left.as_ref() {
                    self.collect_clustering_column_values(
                        left_condition,
                        &clustering_columns,
                        &mut result,
                    );
                }
                self.collect_clustering_column_values(right, &clustering_columns, &mut result);
            }
        }

        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    /// Método auxiliar para recorrer las condiciones y recolectar los valores de las clustering columns.
    fn collect_clustering_column_values(
        &self,
        condition: &Condition,
        clustering_columns: &[String],
        result: &mut Vec<String>,
    ) {
        match condition {
            Condition::Simple {
                field,
                operator,
                value,
            } => {
                // Si la condición simple corresponde a una clustering column
                if clustering_columns.contains(field) && *operator == Operator::Equal {
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
                        self.collect_clustering_column_values(
                            left_condition,
                            clustering_columns,
                            result,
                        );
                    }
                    self.collect_clustering_column_values(right, clustering_columns, result);
                }
            }
        }
    }

    /// Obtiene el valor de una clustering column si existe una condición con el operador `=`.
    ///
    /// # Arguments
    ///
    /// * `clustering_column` - El nombre de la clustering column para la que se desea obtener el valor.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(String))` - Si se encuentra una condición `=`.
    /// * `Ok(None)` - Si no hay una condición con `=`.
    /// * `Err(CQLError)` - Si ocurre algún error de validación.
    pub fn get_value_for_clustering_column(&self, clustering_column: &str) -> Option<String> {
        self.recursive_find_equal_condition(&self.condition, clustering_column)
    }

    /// Método recursivo para buscar condiciones `=` para una clustering column específica.
    fn recursive_find_equal_condition(
        &self,
        condition: &Condition,
        clustering_column: &str,
    ) -> Option<String> {
        match condition {
            Condition::Simple {
                field,
                operator,
                value,
            } => {
                if field == clustering_column && *operator == Operator::Equal {
                    return Some(value.clone());
                }
                None
            }
            Condition::Complex {
                left,
                operator,
                right,
            } => {
                // Solo procesar condiciones unidas por `AND`
                if *operator == LogicalOperator::And {
                    if let Some(left_condition) = left {
                        if let Some(value) =
                            self.recursive_find_equal_condition(left_condition, clustering_column)
                        {
                            return Some(value);
                        }
                    }
                    self.recursive_find_equal_condition(right, clustering_column)
                } else {
                    None // Ignorar condiciones con operadores no válidos
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{errors::CQLError, logical_operator::LogicalOperator, operator::Operator};

    #[test]
    fn test_new_from_tokens_simple_condition() {
        let tokens = vec!["WHERE", "age", ">", "18"];
        let where_clause = Where::new_from_tokens(tokens).unwrap();
        assert_eq!(
            where_clause,
            Where {
                condition: Condition::Simple {
                    field: "age".to_string(),
                    operator: Operator::Greater,
                    value: "18".to_string(),
                },
            }
        );
    }

    #[test]
    fn test_new_from_tokens_complex_condition() {
        let tokens = vec!["WHERE", "age", "=", "18", "AND", "name", "=", "John"];
        let where_clause = Where::new_from_tokens(tokens).unwrap();
        assert_eq!(
            where_clause,
            Where {
                condition: Condition::Complex {
                    left: Some(Box::new(Condition::Simple {
                        field: "age".to_string(),
                        operator: Operator::Equal,
                        value: "18".to_string(),
                    })),
                    operator: LogicalOperator::And,
                    right: Box::new(Condition::Simple {
                        field: "name".to_string(),
                        operator: Operator::Equal,
                        value: "John".to_string(),
                    }),
                }
            }
        );
    }

    // #[test]
    // fn test_validate_cql_conditions_valid_update() {
    //     let partitioner_keys = vec!["id".to_string()];
    //     let clustering_columns = vec!["age".to_string(), "name".to_string()];
    //     let condition = Condition::Complex {
    //         left: Some(Box::new(Condition::Simple {
    //             field: "id".to_string(),
    //             operator: Operator::Equal,
    //             value: "1".to_string(),
    //         })),
    //         operator: LogicalOperator::And,
    //         right: Box::new(Condition::Simple {
    //             field: "age".to_string(),
    //             operator: Operator::Equal,
    //             value: "30".to_string(),
    //         }),
    //     };

    //     let where_clause = Where { condition };
    //     assert!(where_clause
    //         .validate_cql_conditions(&partitioner_keys, &clustering_columns, false, true)
    //         .is_ok());
    // }

    #[test]
    fn test_validate_cql_conditions_invalid_update_missing_clustering_column() {
        let partitioner_keys = vec!["id".to_string()];
        let clustering_columns = vec!["age".to_string(), "name".to_string()];
        let condition = Condition::Simple {
            field: "id".to_string(),
            operator: Operator::Equal,
            value: "1".to_string(),
        };

        let where_clause = Where { condition };
        assert_eq!(
            where_clause.validate_cql_conditions(
                &partitioner_keys,
                &clustering_columns,
                false,
                true
            ),
            Err(CQLError::InvalidCondition)
        );
    }

    #[test]
    fn test_get_value_partitioner_key_condition_single_key() {
        let partitioner_keys = vec!["id".to_string()];
        let condition = Condition::Simple {
            field: "id".to_string(),
            operator: Operator::Equal,
            value: "123".to_string(),
        };

        let where_clause = Where { condition };
        let result = where_clause.get_value_partitioner_key_condition(partitioner_keys);
        assert_eq!(result, Ok(vec!["123".to_string()]));
    }

    #[test]
    fn test_get_value_partitioner_key_condition_multiple_keys() {
        let partitioner_keys = vec!["id".to_string(), "key".to_string()];
        let condition = Condition::Complex {
            left: Some(Box::new(Condition::Simple {
                field: "id".to_string(),
                operator: Operator::Equal,
                value: "123".to_string(),
            })),
            operator: LogicalOperator::And,
            right: Box::new(Condition::Simple {
                field: "key".to_string(),
                operator: Operator::Equal,
                value: "abc".to_string(),
            }),
        };

        let where_clause = Where { condition };
        let result = where_clause.get_value_partitioner_key_condition(partitioner_keys);
        assert_eq!(result, Ok(vec!["123".to_string(), "abc".to_string()]));
    }

    #[test]
    fn test_get_value_clustering_column_condition_single_column() {
        let clustering_columns = vec!["age".to_string()];
        let condition = Condition::Simple {
            field: "age".to_string(),
            operator: Operator::Equal,
            value: "25".to_string(),
        };

        let where_clause = Where { condition };
        let result = where_clause.get_value_clustering_column_condition(clustering_columns);
        assert_eq!(result, Some(vec!["25".to_string()]));
    }

    #[test]
    fn test_get_value_clustering_column_condition_multiple_columns() {
        let clustering_columns = vec!["age".to_string(), "name".to_string()];
        let condition = Condition::Complex {
            left: Some(Box::new(Condition::Simple {
                field: "age".to_string(),
                operator: Operator::Equal,
                value: "25".to_string(),
            })),
            operator: LogicalOperator::And,
            right: Box::new(Condition::Simple {
                field: "name".to_string(),
                operator: Operator::Equal,
                value: "John".to_string(),
            }),
        };

        let where_clause = Where { condition };
        let result = where_clause.get_value_clustering_column_condition(clustering_columns);
        assert_eq!(result, Some(vec!["25".to_string(), "John".to_string()]));
    }

    #[test]
    fn test_get_value_partitioner_key_condition_no_match() {
        let partitioner_keys = vec!["id".to_string()];
        let condition = Condition::Simple {
            field: "age".to_string(),
            operator: Operator::Equal,
            value: "30".to_string(),
        };

        let where_clause = Where { condition };
        let result = where_clause.get_value_partitioner_key_condition(partitioner_keys);
        assert_eq!(result, Err(CQLError::InvalidColumn));
    }

    #[test]
    fn test_get_value_clustering_column_condition_no_match() {
        let clustering_columns = vec!["age".to_string()];
        let condition = Condition::Simple {
            field: "name".to_string(),
            operator: Operator::Equal,
            value: "Alice".to_string(),
        };

        let where_clause = Where { condition };
        let result = where_clause.get_value_clustering_column_condition(clustering_columns);
        assert_eq!(result, None);
    }

    #[test]
    fn test_simple_condition_equal() {
        let where_clause = Where {
            condition: Condition::Simple {
                field: "value1".to_string(),
                operator: Operator::Equal,
                value: "150".to_string(),
            },
        };

        let result = where_clause.get_value_for_clustering_column("value1");
        assert_eq!(result, Some("150".to_string()));
    }

    #[test]
    fn test_simple_condition_non_equal() {
        let where_clause = Where {
            condition: Condition::Simple {
                field: "value1".to_string(),
                operator: Operator::Greater,
                value: "300".to_string(),
            },
        };

        let result = where_clause.get_value_for_clustering_column("value1");
        assert_eq!(result, None);
    }

    #[test]
    fn test_simple_condition_different_column() {
        let where_clause = Where {
            condition: Condition::Simple {
                field: "value2".to_string(),
                operator: Operator::Equal,
                value: "500".to_string(),
            },
        };

        let result = where_clause.get_value_for_clustering_column("value1");
        assert_eq!(result, None);
    }

    #[test]
    fn test_complex_condition_with_and_equal() {
        let where_clause = Where {
            condition: Condition::Complex {
                left: Some(Box::new(Condition::Simple {
                    field: "value1".to_string(),
                    operator: Operator::Equal,
                    value: "150".to_string(),
                })),
                operator: LogicalOperator::And,
                right: Box::new(Condition::Simple {
                    field: "value2".to_string(),
                    operator: Operator::Greater,
                    value: "300".to_string(),
                }),
            },
        };

        let result = where_clause.get_value_for_clustering_column("value1");
        assert_eq!(result, Some("150".to_string()));

        let result = where_clause.get_value_for_clustering_column("value2");
        assert_eq!(result, None);
    }

    #[test]
    fn test_complex_condition_with_multiple_and() {
        let where_clause = Where {
            condition: Condition::Complex {
                left: Some(Box::new(Condition::Simple {
                    field: "value1".to_string(),
                    operator: Operator::Equal,
                    value: "150".to_string(),
                })),
                operator: LogicalOperator::And,
                right: Box::new(Condition::Complex {
                    left: Some(Box::new(Condition::Simple {
                        field: "value2".to_string(),
                        operator: Operator::Equal,
                        value: "500".to_string(),
                    })),
                    operator: LogicalOperator::And,
                    right: Box::new(Condition::Simple {
                        field: "value3".to_string(),
                        operator: Operator::Greater,
                        value: "40".to_string(),
                    }),
                }),
            },
        };

        let result = where_clause.get_value_for_clustering_column("value1");
        assert_eq!(result, Some("150".to_string()));

        let result = where_clause.get_value_for_clustering_column("value2");
        assert_eq!(result, Some("500".to_string()));

        let result = where_clause.get_value_for_clustering_column("value3");
        assert_eq!(result, None);
    }

    #[test]
    fn test_complex_condition_with_or() {
        let where_clause = Where {
            condition: Condition::Complex {
                left: Some(Box::new(Condition::Simple {
                    field: "value1".to_string(),
                    operator: Operator::Equal,
                    value: "150".to_string(),
                })),
                operator: LogicalOperator::Or,
                right: Box::new(Condition::Simple {
                    field: "value2".to_string(),
                    operator: Operator::Equal,
                    value: "500".to_string(),
                }),
            },
        };

        let result = where_clause.get_value_for_clustering_column("value1");
        assert_eq!(result, None);

        let result = where_clause.get_value_for_clustering_column("value2");
        assert_eq!(result, None);
    }

    #[test]
    fn test_no_conditions() {
        let where_clause = Where {
            condition: Condition::Complex {
                left: None,
                operator: LogicalOperator::And,
                right: Box::new(Condition::Simple {
                    field: "value1".to_string(),
                    operator: Operator::Greater,
                    value: "150".to_string(),
                }),
            },
        };

        let result = where_clause.get_value_for_clustering_column("value1");
        assert_eq!(result, None);
    }

    #[test]
    fn test_invalid_condition_for_column() {
        let where_clause = Where {
            condition: Condition::Simple {
                field: "value4".to_string(),
                operator: Operator::Equal,
                value: "999".to_string(),
            },
        };

        let result = where_clause.get_value_for_clustering_column("value1");
        assert_eq!(result, None);
    }
}
