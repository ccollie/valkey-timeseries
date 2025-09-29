/// A naive 2-dimensional array implementation using nested Vecs
/// Used to avoid bringing in ndarray or similar dependencies
#[derive(Debug, Clone, PartialEq)]
pub struct Array2D<T> {
    data: Vec<Vec<T>>,
    rows: usize,
    cols: usize,
}

impl<T: Clone + Default> Array2D<T> {
    /// Create a new Array2D with the specified dimensions, filled with default values
    pub fn new(rows: usize, cols: usize) -> Self {
        let data = vec![vec![T::default(); cols]; rows];
        Self { data, rows, cols }
    }

    /// Create a new Array2D with the specified dimensions, filled with the given value
    pub fn filled(rows: usize, cols: usize, value: T) -> Self {
        let data = vec![vec![value; cols]; rows];
        Self { data, rows, cols }
    }

    /// Create a new Array2D from a vector of vectors
    /// Returns None if the input is invalid (empty or inconsistent row lengths)
    pub fn from_vec(data: Vec<Vec<T>>) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        let rows = data.len();
        let cols = data[0].len();

        // Check that all rows have the same length
        if data.iter().any(|row| row.len() != cols) {
            return None;
        }

        Some(Self { data, rows, cols })
    }

    /// Get the number of rows
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Get the number of columns
    pub fn cols(&self) -> usize {
        self.cols
    }

    /// Get the dimensions as a tuple (rows, cols)
    pub fn shape(&self) -> (usize, usize) {
        (self.rows, self.cols)
    }

    /// Get a reference to the element at the specified position
    /// Returns None if the indices are out of bounds
    pub fn get(&self, row: usize, col: usize) -> Option<&T> {
        self.data.get(row)?.get(col)
    }

    /// Get a mutable reference to the element at the specified position
    /// Returns None if the indices are out of bounds
    pub fn get_mut(&mut self, row: usize, col: usize) -> Option<&mut T> {
        self.data.get_mut(row)?.get_mut(col)
    }

    /// Set the value at the specified position
    /// Returns the old value if successful, None if indices are out of bounds
    pub fn set(&mut self, row: usize, col: usize, value: T) -> Option<T> {
        let old_value = std::mem::replace(self.get_mut(row, col)?, value);
        Some(old_value)
    }

    /// Get a reference to an entire row
    /// Returns None if the row index is out of bounds
    pub fn get_row(&self, row: usize) -> Option<&Vec<T>> {
        self.data.get(row)
    }

    /// Get a mutable reference to an entire row
    /// Returns None if the row index is out of bounds
    pub fn get_row_mut(&mut self, row: usize) -> Option<&mut Vec<T>> {
        self.data.get_mut(row)
    }

    /// Get a column as a new vector
    /// Returns None if the column index is out of bounds
    pub fn get_column(&self, col: usize) -> Option<Vec<T>> {
        if col >= self.cols {
            return None;
        }

        let column: Vec<T> = self.data.iter().map(|row| row[col].clone()).collect();
        Some(column)
    }

    /// Set an entire row
    /// Returns false if the row index is out of bounds or the new row has wrong length
    pub fn set_row(&mut self, row: usize, new_row: Vec<T>) -> bool {
        if row >= self.rows || new_row.len() != self.cols {
            return false;
        }

        self.data[row] = new_row;
        true
    }

    /// Set an entire column
    /// Returns false if the column index is out of bounds or the new column has the wrong length
    pub fn set_column(&mut self, col: usize, new_column: Vec<T>) -> bool {
        if col >= self.cols || new_column.len() != self.rows {
            return false;
        }

        for (row_idx, value) in new_column.into_iter().enumerate() {
            self.data[row_idx][col] = value;
        }
        true
    }

    /// Iterate over all elements row by row
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.data.iter().flat_map(|row| row.iter())
    }

    /// Iterate over all elements row by row (mutable)
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.data.iter_mut().flat_map(|row| row.iter_mut())
    }

    /// Iterate over rows
    pub fn iter_rows(&self) -> impl Iterator<Item = &Vec<T>> {
        self.data.iter()
    }

    /// Iterate over rows (mutable)
    pub fn iter_rows_mut(&mut self) -> impl Iterator<Item = &mut Vec<T>> {
        self.data.iter_mut()
    }

    /// Convert to a flat vector (row-major order)
    pub fn get_flat_vec(&self) -> Vec<T> {
        self.data.iter().flatten().cloned().collect()
    }

    /// Convert to the underlying Vec<Vec<T>>
    pub fn into_inner(self) -> Vec<Vec<T>> {
        self.data
    }

    /// Get a reference to the underlying data
    pub fn as_inner(&self) -> &Vec<Vec<T>> {
        &self.data
    }
}

// Index trait implementation for convenient access
impl<T> std::ops::Index<(usize, usize)> for Array2D<T> {
    type Output = T;

    fn index(&self, (row, col): (usize, usize)) -> &Self::Output {
        &self.data[row][col]
    }
}

impl<T> std::ops::IndexMut<(usize, usize)> for Array2D<T> {
    fn index_mut(&mut self, (row, col): (usize, usize)) -> &mut Self::Output {
        &mut self.data[row][col]
    }
}

// Display trait for pretty printing
impl<T: std::fmt::Display> std::fmt::Display for Array2D<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, row) in self.data.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            for (j, item) in row.iter().enumerate() {
                if j > 0 {
                    write!(f, " ")?;
                }
                write!(f, "{item}")?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_array2d() {
        let arr: Array2D<i32> = Array2D::new(3, 4);
        assert_eq!(arr.rows(), 3);
        assert_eq!(arr.cols(), 4);
        assert_eq!(arr.shape(), (3, 4));
    }

    #[test]
    fn test_filled_array2d() {
        let arr = Array2D::filled(2, 3, 42);
        assert_eq!(arr[(0, 0)], 42);
        assert_eq!(arr[(1, 2)], 42);
    }

    #[test]
    fn test_from_vec() {
        let data = vec![
            vec![1, 2, 3],
            vec![4, 5, 6],
        ];
        let arr = Array2D::from_vec(data).unwrap();
        assert_eq!(arr[(0, 0)], 1);
        assert_eq!(arr[(1, 2)], 6);
    }

    #[test]
    fn test_get_set() {
        let mut arr = Array2D::filled(2, 2, 0);
        assert_eq!(arr.get(0, 0), Some(&0));
        assert_eq!(arr.get(2, 0), None); // Out of bounds

        let old_value = arr.set(0, 1, 42);
        assert_eq!(old_value, Some(0));
        assert_eq!(arr[(0, 1)], 42);
    }

    #[test]
    fn test_row_operations() {
        let mut arr = Array2D::from_vec(vec![
            vec![1, 2, 3],
            vec![4, 5, 6],
        ]).unwrap();

        assert_eq!(arr.get_row(0), Some(&vec![1, 2, 3]));
        assert_eq!(arr.get_column(1), Some(vec![2, 5]));

        assert!(arr.set_row(1, vec![7, 8, 9]));
        assert_eq!(arr[(1, 0)], 7);
        assert_eq!(arr[(1, 2)], 9);

        assert!(arr.set_column(0, vec![10, 11]));
        assert_eq!(arr[(0, 0)], 10);
        assert_eq!(arr[(1, 0)], 11);
    }

    #[test]
    fn test_iterators() {
        let arr = Array2D::from_vec(vec![
            vec![1, 2],
            vec![3, 4],
        ]).unwrap();

        let flat: Vec<i32> = arr.iter().cloned().collect();
        assert_eq!(flat, vec![1, 2, 3, 4]);

        let row_count = arr.iter_rows().count();
        assert_eq!(row_count, 2);
    }

    #[test]
    fn test_display() {
        let arr = Array2D::from_vec(vec![
            vec![1, 2, 3],
            vec![4, 5, 6],
        ]).unwrap();

        let displayed = format!("{}", arr);
        assert_eq!(displayed, "1 2 3\n4 5 6");
    }
}