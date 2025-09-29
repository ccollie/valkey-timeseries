mod anomalies;
mod double_mad_outlier_detector;
mod mad_outlier_detector;
mod smoothed_zscores;
#[cfg(test)]
mod outlier_test_data;
#[cfg(test)]
mod mad_outlier_detector_tests;
#[cfg(test)]
mod double_mad_outlier_detector_tests;
mod rcf_outlier_detector;

use std::ops::Deref;
pub use anomalies::*;
pub use double_mad_outlier_detector::*;


pub trait OutlierDetector {
    // Check if the value is a lower outlier
    fn is_lower_outlier(&self, x: f64) -> bool;

    // Check if the value is an upper outlier
    fn is_upper_outlier(&self, x: f64) -> bool;
}

impl<T> OutlierDetector for Box<T>
where T: OutlierDetector
{
    fn is_lower_outlier(&self, x: f64) -> bool {
        self.deref().is_lower_outlier(x)
    }

    fn is_upper_outlier(&self, x: f64) -> bool {
        self.deref().is_upper_outlier(x)
    }
}