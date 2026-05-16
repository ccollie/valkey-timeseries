/// Stat holds values for a single cardinality statistic.
#[derive(Debug, Clone, Default)]
pub struct PostingStat {
    pub name: String,
    pub count: u64,
}

#[derive(Clone, Debug, Default)]
pub struct PostingsStats {
    pub series_count_by_metric_name: Vec<PostingStat>,
    pub series_count_by_label_name: Vec<PostingStat>,
    pub series_count_by_label_value_pairs: Vec<PostingStat>,
    pub series_count_by_focus_label_value: Option<Vec<PostingStat>>,
    pub total_label_value_pairs: usize,
    pub label_count: usize,
    pub series_count: u64,
}

pub(crate) struct StatsMaxHeap {
    max_length: usize,
    min_value: usize,
    min_index: usize,
    items: Vec<PostingStat>,
}

impl StatsMaxHeap {
    pub fn new(length: usize) -> Self {
        StatsMaxHeap {
            max_length: length,
            min_value: usize::MAX,
            min_index: 0,
            items: Vec::with_capacity(length),
        }
    }

    pub fn push(&mut self, item: PostingStat) {
        if self.items.len() < self.max_length {
            if item.count < self.min_value as u64 {
                self.min_value = item.count as usize;
                self.min_index = self.items.len();
            }
            self.items.push(item);
            return;
        }
        if item.count < self.min_value as u64 {
            return;
        }

        self.min_value = item.count as usize;
        self.items[self.min_index] = item;

        for (i, stat) in self.items.iter().enumerate() {
            if stat.count < self.min_value as u64 {
                self.min_value = stat.count as usize;
                self.min_index = i;
            }
        }
    }

    pub fn into_vec(self) -> Vec<PostingStat> {
        self.items
    }
}
