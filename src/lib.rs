use std::collections::HashMap;
use std::sync::Arc;
use anyhow::Result;
use arrow::array::{Float64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use ndarray::{Array, Array2, ArrayView2};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub mod server;
pub mod matrix;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Matrix {
    pub data: Vec<f64>,
    pub shape: Vec<usize>,
}

impl Matrix {
    pub fn new(data: Vec<f64>, shape: Vec<usize>) -> Self {
        Self { data, shape }
    }

    pub fn to_ndarray(&self) -> Result<Array2<f64>> {
        if self.shape.len() != 2 {
            return Err(anyhow::anyhow!("Only 2D matrices are supported"));
        }
        let rows = self.shape[0];
        let cols = self.shape[1];
        Ok(Array::from_shape_vec((rows, cols), self.data.clone())?)
    }

    pub fn from_ndarray(array: ArrayView2<f64>) -> Self {
        let shape = array.shape().to_vec();
        let data = array.iter().cloned().collect();
        Self { data, shape }
    }

    pub fn to_arrow_record_batch(&self) -> Result<RecordBatch> {
        if self.shape.len() != 2 {
            return Err(anyhow::anyhow!("Only 2D matrices are supported"));
        }

        let rows = self.shape[0];
        let cols = self.shape[1];
        
        // Create schema with column names
        let fields: Vec<Field> = (0..cols)
            .map(|i| Field::new(&format!("col_{}", i), DataType::Float64, false))
            .collect();
        let schema = Schema::new(fields);

        // Create arrays for each column
        let arrays: Vec<arrow::array::ArrayRef> = (0..cols)
            .map(|col| {
                let col_data: Vec<f64> = (0..rows)
                    .map(|row| self.data[row * cols + col])
                    .collect();
                Arc::new(Float64Array::from(col_data)) as arrow::array::ArrayRef
            })
            .collect();

        Ok(RecordBatch::try_new(Arc::new(schema), arrays)?)
    }

    pub fn from_arrow_record_batch(batch: &RecordBatch) -> Result<Self> {
        let schema = batch.schema();
        let num_rows = batch.num_rows();
        let num_cols = schema.fields().len();

        let mut data = Vec::with_capacity(num_rows * num_cols);
        
        for row in 0..num_rows {
            for col in 0..num_cols {
                let array = batch.column(col);
                if let Some(value) = array.as_any().downcast_ref::<Float64Array>() {
                    data.push(value.value(row));
                } else {
                    return Err(anyhow::anyhow!("Column {} is not Float64", col));
                }
            }
        }

        Ok(Self {
            data,
            shape: vec![num_rows, num_cols],
        })
    }
}

#[derive(Debug, Clone)]
pub struct KVStore {
    store: Arc<DashMap<Uuid, Matrix>>,
}

impl KVStore {
    pub fn new() -> Self {
        Self {
            store: Arc::new(DashMap::new()),
        }
    }

    pub fn insert(&self, key: Uuid, matrix: Matrix) -> Option<Matrix> {
        self.store.insert(key, matrix)
    }

    pub fn get(&self, key: &Uuid) -> Option<Matrix> {
        self.store.get(key).map(|entry| entry.clone())
    }

    pub fn remove(&self, key: &Uuid) -> Option<Matrix> {
        self.store.remove(key).map(|(_, matrix)| matrix)
    }

    pub fn contains_key(&self, key: &Uuid) -> bool {
        self.store.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.store.len()
    }

    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    pub fn keys(&self) -> Vec<Uuid> {
        self.store.iter().map(|entry| *entry.key()).collect()
    }

    pub fn clear(&self) {
        self.store.clear();
    }
}

impl Default for KVStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::array;

    #[test]
    fn test_matrix_creation() {
        let data = vec![1.0, 2.0, 3.0, 4.0];
        let shape = vec![2, 2];
        let matrix = Matrix::new(data.clone(), shape.clone());
        
        assert_eq!(matrix.data, data);
        assert_eq!(matrix.shape, shape);
    }

    #[test]
    fn test_matrix_to_ndarray() {
        let data = vec![1.0, 2.0, 3.0, 4.0];
        let shape = vec![2, 2];
        let matrix = Matrix::new(data, shape);
        
        let array = matrix.to_ndarray().unwrap();
        let expected = array![[1.0, 2.0], [3.0, 4.0]];
        assert_eq!(array, expected);
    }

    #[test]
    fn test_matrix_from_ndarray() {
        let array = array![[1.0, 2.0], [3.0, 4.0]];
        let matrix = Matrix::from_ndarray(array.view());
        
        assert_eq!(matrix.data, vec![1.0, 2.0, 3.0, 4.0]);
        assert_eq!(matrix.shape, vec![2, 2]);
    }

    #[test]
    fn test_kv_store_operations() {
        let store = KVStore::new();
        let key = Uuid::new_v4();
        let matrix = Matrix::new(vec![1.0, 2.0, 3.0, 4.0], vec![2, 2]);

        // Test insert and get
        assert!(store.insert(key, matrix.clone()).is_none());
        let got = store.get(&key);
        assert!(got.is_some());
        let got_matrix = got.unwrap();
        assert_eq!(got_matrix.data, matrix.data);
        assert_eq!(got_matrix.shape, matrix.shape);

        // Test contains_key
        assert!(store.contains_key(&key));

        // Test remove
        let removed = store.remove(&key);
        assert!(removed.is_some());
        let removed_matrix = removed.unwrap();
        assert_eq!(removed_matrix.data, matrix.data);
        assert_eq!(removed_matrix.shape, matrix.shape);
        assert!(!store.contains_key(&key));
    }

    #[test]
    fn test_arrow_serialization() {
        let matrix = Matrix::new(vec![1.0, 2.0, 3.0, 4.0], vec![2, 2]);
        let batch = matrix.to_arrow_record_batch().unwrap();
        let deserialized = Matrix::from_arrow_record_batch(&batch).unwrap();
        
        assert_eq!(matrix.data, deserialized.data);
        assert_eq!(matrix.shape, deserialized.shape);
    }
} 