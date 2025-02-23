use crate::common::Sample;
use crate::join::JoinValue;
use joinkit::{EitherOrBoth, Joinkit};
use std::collections::VecDeque;
// todo: this seems inefficient

pub struct JoinRightIter<'a> {
    buf: VecDeque<JoinValue>,
    inner: Box<dyn Iterator<Item = EitherOrBoth<&'a Sample, Vec<&'a Sample>>> + 'a>,
}

impl<'a> JoinRightIter<'a> {
    // todo: pass in impl Iterator<Item=Sample>
    pub(crate) fn new(left: &'a [Sample], right: &'a [Sample]) -> Self {
        let left_iter = left.iter().map(|sample| (sample.timestamp, sample));
        let right_iter = right.iter().map(|sample| (sample.timestamp, sample));
        let iter = left_iter.into_iter().hash_join_right_outer(right_iter);

        Self {
            buf: Default::default(),
            inner: Box::new(iter),
        }
    }

    fn next_from_buf(&mut self) -> Option<JoinValue> {
        self.buf.pop_front()
    }

    fn process_item(
        &mut self,
        item: EitherOrBoth<&'a Sample, Vec<&'a Sample>>,
    ) -> Option<JoinValue> {
        match item {
            EitherOrBoth::Left(_) => {
                // should not happen
                None
            }
            EitherOrBoth::Right(r) => {
                let items = &r[0..];
                if r.len() == 1 {
                    let sample = items[0];
                    return Some(JoinValue::right(sample.timestamp, sample.value));
                }
                for row in r.iter() {
                    self.buf
                        .push_back(JoinValue::right(row.timestamp, row.value))
                }
                self.next_from_buf()
            }
            EitherOrBoth::Both(left, right) => {
                let ts = left.timestamp;
                for sample in right.iter() {
                    let row = JoinValue::both(ts, left.value, sample.value);
                    self.buf.push_back(row);
                }
                self.next_from_buf()
            }
        }
    }
}

impl Iterator for JoinRightIter<'_> {
    type Item = JoinValue;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next();
        match item {
            None => self.next_from_buf(),
            Some(value) => self.process_item(value),
        }
    }
}
