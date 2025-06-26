use anyhow::Result;
use ndarray::{Array2, ArrayView2, Axis};
use crate::Matrix;

pub trait MatrixOps {
    fn add(&self, other: &Matrix) -> Result<Matrix>;
    fn subtract(&self, other: &Matrix) -> Result<Matrix>;
    fn multiply(&self, other: &Matrix) -> Result<Matrix>;
    fn transpose(&self) -> Result<Matrix>;
    fn scale(&self, factor: f64) -> Matrix;
    fn sum(&self) -> f64;
    fn mean(&self) -> f64;
    fn max(&self) -> f64;
    fn min(&self) -> f64;
}

impl MatrixOps for Matrix {
    fn add(&self, other: &Matrix) -> Result<Matrix> {
        if self.shape != other.shape {
            return Err(anyhow::anyhow!("Matrix shapes must match for addition"));
        }
        
        let result_data: Vec<f64> = self.data
            .iter()
            .zip(other.data.iter())
            .map(|(a, b)| a + b)
            .collect();
            
        Ok(Matrix::new(result_data, self.shape.clone()))
    }

    fn subtract(&self, other: &Matrix) -> Result<Matrix> {
        if self.shape != other.shape {
            return Err(anyhow::anyhow!("Matrix shapes must match for subtraction"));
        }
        
        let result_data: Vec<f64> = self.data
            .iter()
            .zip(other.data.iter())
            .map(|(a, b)| a - b)
            .collect();
            
        Ok(Matrix::new(result_data, self.shape.clone()))
    }

    fn multiply(&self, other: &Matrix) -> Result<Matrix> {
        if self.shape.len() != 2 || other.shape.len() != 2 {
            return Err(anyhow::anyhow!("Only 2D matrices supported for multiplication"));
        }
        
        if self.shape[1] != other.shape[0] {
            return Err(anyhow::anyhow!("Matrix dimensions incompatible for multiplication"));
        }
        
        let a = self.to_ndarray()?;
        let b = other.to_ndarray()?;
        let result = a.dot(&b);
        
        Ok(Matrix::from_ndarray(result.view()))
    }

    fn transpose(&self) -> Result<Matrix> {
        if self.shape.len() != 2 {
            return Err(anyhow::anyhow!("Only 2D matrices supported for transpose"));
        }
        
        let array = self.to_ndarray()?;
        let transposed = array.t();
        
        Ok(Matrix::from_ndarray(transposed.view()))
    }

    fn scale(&self, factor: f64) -> Matrix {
        let result_data: Vec<f64> = self.data
            .iter()
            .map(|x| x * factor)
            .collect();
            
        Matrix::new(result_data, self.shape.clone())
    }

    fn sum(&self) -> f64 {
        self.data.iter().sum()
    }

    fn mean(&self) -> f64 {
        if self.data.is_empty() {
            return 0.0;
        }
        self.sum() / self.data.len() as f64
    }

    fn max(&self) -> f64 {
        self.data.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b))
    }

    fn min(&self) -> f64 {
        self.data.iter().fold(f64::INFINITY, |a, &b| a.min(b))
    }
}

pub fn create_random_matrix(rows: usize, cols: usize) -> Matrix {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    
    let data: Vec<f64> = (0..rows * cols)
        .map(|_| rng.gen_range(-1.0..1.0))
        .collect();
        
    Matrix::new(data, vec![rows, cols])
}

pub fn create_identity_matrix(size: usize) -> Matrix {
    let mut data = vec![0.0; size * size];
    for i in 0..size {
        data[i * size + i] = 1.0;
    }
    Matrix::new(data, vec![size, size])
}

pub fn create_zeros_matrix(rows: usize, cols: usize) -> Matrix {
    let data = vec![0.0; rows * cols];
    Matrix::new(data, vec![rows, cols])
}

pub fn create_ones_matrix(rows: usize, cols: usize) -> Matrix {
    let data = vec![1.0; rows * cols];
    Matrix::new(data, vec![rows, cols])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_matrix_addition() {
        let a = Matrix::new(vec![1.0, 2.0, 3.0, 4.0], vec![2, 2]);
        let b = Matrix::new(vec![5.0, 6.0, 7.0, 8.0], vec![2, 2]);
        
        let result = a.add(&b).unwrap();
        assert_eq!(result.data, vec![6.0, 8.0, 10.0, 12.0]);
    }

    #[test]
    fn test_matrix_multiplication() {
        let a = Matrix::new(vec![1.0, 2.0, 3.0, 4.0], vec![2, 2]);
        let b = Matrix::new(vec![5.0, 6.0, 7.0, 8.0], vec![2, 2]);
        
        let result = a.multiply(&b).unwrap();
        assert_eq!(result.data, vec![19.0, 22.0, 43.0, 50.0]);
    }

    #[test]
    fn test_matrix_transpose() {
        let matrix = Matrix::new(vec![1.0, 2.0, 3.0, 4.0], vec![2, 2]);
        let transposed = matrix.transpose().unwrap();
        
        assert_eq!(transposed.data, vec![1.0, 3.0, 2.0, 4.0]);
        assert_eq!(transposed.shape, vec![2, 2]);
    }

    #[test]
    fn test_identity_matrix() {
        let identity = create_identity_matrix(3);
        assert_eq!(identity.data, vec![1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0]);
        assert_eq!(identity.shape, vec![3, 3]);
    }
} 