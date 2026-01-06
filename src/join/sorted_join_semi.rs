pub struct SortedJoinSemiBy<L, R, K, F> {
    left: L,
    right: R,
    key_fn: F,
    current_right_key: Option<K>,
}

impl<L, R, K, F> SortedJoinSemiBy<L, R, K, F>
where
    K: Ord + Clone,
{
    pub fn new<LI, RI, I>(left: LI, right: RI, key_fn: F) -> Self
    where
        L: Iterator<Item = I>,
        R: Iterator<Item = I>,
        LI: IntoIterator<IntoIter = L>,
        RI: IntoIterator<IntoIter = R>,
        F: FnMut(&I) -> K + Clone,
    {
        let mut join = SortedJoinSemiBy {
            left: left.into_iter(),
            right: right.into_iter(),
            key_fn,
            current_right_key: None,
        };

        // Initialize with first right key
        if let Some(right_item) = join.right.next() {
            let key = (join.key_fn)(&right_item);
            join.current_right_key = Some(key);
        }

        join
    }
}

impl<L, R, I, K, F> Iterator for SortedJoinSemiBy<L, R, K, F>
where
    L: Iterator<Item = I>,
    R: Iterator<Item = I>,
    K: Ord + Clone,
    F: FnMut(&I) -> K,
{
    type Item = I;

    fn next(&mut self) -> Option<Self::Item> {
        // If right is empty, no matches possible
        self.current_right_key.as_ref()?;

        for left_item in self.left.by_ref() {
            let left_key = (self.key_fn)(&left_item);

            // Advance right iterator until we find a key >= left_key
            while let Some(right_key) = &self.current_right_key {
                match right_key.cmp(&left_key) {
                    std::cmp::Ordering::Less => {
                        // Right key < left key, advance right
                        match self.right.next() {
                            Some(right_item) => {
                                let new_key = (self.key_fn)(&right_item);
                                self.current_right_key = Some(new_key);
                            }
                            None => {
                                self.current_right_key = None;
                                return None; // No more right items, no matches possible
                            }
                        }
                    }
                    std::cmp::Ordering::Equal => {
                        // Found a match
                        return Some(left_item);
                    }
                    std::cmp::Ordering::Greater => {
                        // Right key > left key, no match for this left item
                        break;
                    }
                }
            }
        }

        None
    }
}
