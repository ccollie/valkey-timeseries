/// When grouping, we have the situation where a series occurs in many groups (series share
/// the same label). To calculate aggregations across multiple groups, we cannot use a consuming
/// iterator, since the first group would consume the results from a series.
///
/// Therefore, when grouping, we convert the iterator to a vec and create a simple proxying iterator
/// on top of it.
pub struct SharedVecIter<'a, T: Copy> {
    index: usize,
    inner: &'a Vec<T>,
}

impl<'a, T: Copy> SharedVecIter<'a, T> {
    pub fn new(inner: &'a Vec<T>) -> Self {
        Self { index: 0, inner }
    }
}

impl<T: Copy> Iterator for SharedVecIter<'_, T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.inner.len() {
            return None;
        }
        let item = self.inner.get(self.index);
        self.index += 1;
        item.copied()
    }
}
