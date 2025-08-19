use crate::common::Sample;
use crate::iterators::vec_sample_iterator::VecSampleIterator;
use crate::series::SeriesSampleIterator;
use crate::series::chunks::{ChunkSampleIterator, GorillaChunkIterator, PcoSampleIterator};

#[derive(Default)]
pub enum SampleIter<'a> {
    Series(SeriesSampleIterator<'a>),
    Chunk(ChunkSampleIterator<'a>),
    Slice(std::slice::Iter<'a, Sample>),
    Vec(VecSampleIterator),
    Gorilla(GorillaChunkIterator<'a>),
    Pco(Box<PcoSampleIterator<'a>>),
    #[default]
    Empty,
}

impl<'a> SampleIter<'a> {
    pub fn slice(slice: &'a [Sample]) -> Self {
        let iter = slice.iter();
        SampleIter::Slice(iter)
    }
    pub fn series(iter: SeriesSampleIterator<'a>) -> Self {
        SampleIter::Series(iter)
    }
    pub fn chunk(iter: ChunkSampleIterator<'a>) -> Self {
        SampleIter::Chunk(iter)
    }
    pub fn vec(samples: Vec<Sample>) -> Self {
        SampleIter::Vec(VecSampleIterator::new(samples))
    }
    pub fn gorilla(iter: GorillaChunkIterator<'a>) -> Self {
        SampleIter::Gorilla(iter)
    }
    pub fn pco(iter: PcoSampleIterator<'a>) -> Self {
        SampleIter::Pco(Box::new(iter))
    }
}

impl Iterator for SampleIter<'_> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SampleIter::Series(series) => series.next(),
            SampleIter::Chunk(chunk) => chunk.next(),
            SampleIter::Slice(slice) => slice.next().copied(),
            SampleIter::Vec(iter) => iter.next(),
            SampleIter::Gorilla(iter) => iter.next(),
            SampleIter::Pco(iter) => iter.next(),
            SampleIter::Empty => None,
        }
    }
}

impl<'a> From<SeriesSampleIterator<'a>> for SampleIter<'a> {
    fn from(value: SeriesSampleIterator<'a>) -> Self {
        Self::Series(value)
    }
}

impl<'a> From<ChunkSampleIterator<'a>> for SampleIter<'a> {
    fn from(value: ChunkSampleIterator<'a>) -> Self {
        Self::Chunk(value)
    }
}

impl From<VecSampleIterator> for SampleIter<'_> {
    fn from(value: VecSampleIterator) -> Self {
        Self::Vec(value)
    }
}

impl From<Vec<Sample>> for SampleIter<'_> {
    fn from(value: Vec<Sample>) -> Self {
        Self::Vec(VecSampleIterator::new(value))
    }
}

impl<'a> From<GorillaChunkIterator<'a>> for SampleIter<'a> {
    fn from(value: GorillaChunkIterator<'a>) -> Self {
        Self::Gorilla(value)
    }
}

impl<'a> From<PcoSampleIterator<'a>> for SampleIter<'a> {
    fn from(value: PcoSampleIterator<'a>) -> Self {
        Self::Pco(Box::new(value))
    }
}
