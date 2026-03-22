#[cfg(test)]
mod tests {
    use crate::analysis::outliers::mad_estimator::{
        HarrellDavisNormalizedEstimator, SimpleNormalizedEstimator,
    };
    use crate::analysis::outliers::outlier_test_data::{
        EMPTY_DATASET, SAME_DATASET, TestData, beta_data_set, check_outliers,
        modified_beta_data_set, real_data_set,
    };
    use crate::analysis::quantile_estimators::Samples;
    use std::collections::HashMap;
    use crate::analysis::outliers::double_mad_outlier_detector::DoubleMadOutlierDetector;

    /// Data cases for SimpleQuantileEstimator
    fn simple_qe_test_data_map() -> HashMap<&'static str, TestData<'static>> {
        let mut map = HashMap::new();
        map.insert("Empty", TestData::new(&EMPTY_DATASET, &[]));
        map.insert("Same", TestData::new(&SAME_DATASET, &[]));
        map.insert(
            "Case1",
            TestData::new(
                &[
                    1.0, 4.0, 4.0, 4.0, 5.0, 5.0, 5.0, 5.0, 7.0, 7.0, 8.0, 10.0, 16.0, 30.0,
                ],
                &[1.0, 16.0, 30.0],
            ),
        );
        map.insert(
            "Real0",
            TestData::new(&real_data_set::X0, &[38594.0, 39075.0]),
        );
        map.insert(
            "Real1",
            TestData::new(&real_data_set::X1, &[0.0, 0.0, 0.0, 0.0, 1821.0]),
        );
        map.insert("Real2", TestData::new(&real_data_set::X2, &[95.0, 4364.0]));
        map.insert(
            "Real3",
            TestData::new(
                &real_data_set::X3,
                &[1067.0, 1085.0, 1133.0, 1643.0, 4642.0],
            ),
        );
        map.insert("Real4", TestData::new(&real_data_set::X4, &[]));
        map.insert("Beta0", TestData::new(&beta_data_set::X0, &[]));
        map.insert("Beta1", TestData::new(&beta_data_set::X1, &[3071.0]));
        map.insert("Beta2", TestData::new(&beta_data_set::X2, &[3642.0]));
        map.insert(
            "MBeta_Lower1",
            TestData::new(&modified_beta_data_set::LOWER1, &[-2000.0, 3612.0]),
        );
        map.insert(
            "MBeta_Lower2",
            TestData::new(&modified_beta_data_set::LOWER2, &[-2001.0, -2000.0, 3612.0]),
        );
        map.insert(
            "MBeta_Lower3",
            TestData::new(
                &modified_beta_data_set::LOWER3,
                &[-2002.0, -2001.0, -2000.0, 3612.0],
            ),
        );
        map.insert(
            "MBeta_Upper1",
            TestData::new(&modified_beta_data_set::UPPER1, &[3612.0, 6000.0]),
        );
        map.insert(
            "MBeta_Upper2",
            TestData::new(&modified_beta_data_set::UPPER2, &[6000.0, 6001.0]),
        );
        map.insert(
            "MBeta_Upper3",
            TestData::new(&modified_beta_data_set::UPPER3, &[6000.0, 6001.0, 6002.0]),
        );
        map.insert(
            "MBeta_Both0",
            TestData::new(&modified_beta_data_set::BOTH0, &[-2000.0, 6000.0]),
        );
        map.insert(
            "MBeta_Both1",
            TestData::new(
                &modified_beta_data_set::BOTH1,
                &[-2001.0, -2000.0, 6000.0, 6001.0],
            ),
        );
        map.insert(
            "MBeta_Both2",
            TestData::new(
                &modified_beta_data_set::BOTH2,
                &[-2002.0, -2001.0, -2000.0, 6000.0, 6001.0, 6002.0],
            ),
        );
        map
    }

    /// Data cases for HarrellDavisQuantileEstimator
    fn hd_qe_test_data_map() -> HashMap<&'static str, TestData<'static>> {
        let mut map = HashMap::new();
        map.insert("Empty", TestData::new(&EMPTY_DATASET, &[]));
        map.insert("Same", TestData::new(&SAME_DATASET, &[]));
        map.insert(
            "Real0",
            TestData::new(&real_data_set::X0, &[38594.0, 39075.0]),
        );
        map.insert(
            "Real1",
            TestData::new(&real_data_set::X1, &[0.0, 0.0, 0.0, 0.0, 1821.0]),
        );
        map.insert("Real2", TestData::new(&real_data_set::X2, &[95.0, 4364.0]));
        map.insert(
            "Real3",
            TestData::new(
                &real_data_set::X3,
                &[1067.0, 1085.0, 1133.0, 1643.0, 4642.0],
            ),
        );
        map.insert("Real4", TestData::new(&real_data_set::X4, &[]));
        map.insert("Beta0", TestData::new(&beta_data_set::X0, &[]));
        map.insert("Beta1", TestData::new(&beta_data_set::X1, &[3071.0]));
        map.insert("Beta2", TestData::new(&beta_data_set::X2, &[3642.0]));
        map.insert(
            "MBeta_Lower1",
            TestData::new(&modified_beta_data_set::LOWER1, &[-2000.0]),
        );
        map.insert(
            "MBeta_Lower2",
            TestData::new(&modified_beta_data_set::LOWER2, &[-2001.0, -2000.0]),
        );
        map.insert(
            "MBeta_Lower3",
            TestData::new(
                &modified_beta_data_set::LOWER3,
                &[-2002.0, -2001.0, -2000.0],
            ),
        );
        map.insert(
            "MBeta_Upper1",
            TestData::new(&modified_beta_data_set::UPPER1, &[6000.0]),
        );
        map.insert(
            "MBeta_Upper2",
            TestData::new(&modified_beta_data_set::UPPER2, &[6000.0, 6001.0]),
        );
        map.insert(
            "MBeta_Upper3",
            TestData::new(&modified_beta_data_set::UPPER3, &[6000.0, 6001.0, 6002.0]),
        );
        map.insert(
            "MBeta_Both0",
            TestData::new(&modified_beta_data_set::BOTH0, &[-2000.0, 6000.0]),
        );
        map.insert(
            "MBeta_Both1",
            TestData::new(
                &modified_beta_data_set::BOTH1,
                &[-2001.0, -2000.0, 6000.0, 6001.0],
            ),
        );
        map.insert(
            "MBeta_Both2",
            TestData::new(
                &modified_beta_data_set::BOTH2,
                &[-2002.0, -2001.0, -2000.0, 6000.0, 6001.0, 6002.0],
            ),
        );
        map
    }

    #[test]
    fn double_mad_outlier_detector_simple_qe_test() {
        let test_data_map = simple_qe_test_data_map();

        for (test_data_key, test_data) in test_data_map.iter() {
            let action = || {
                check_outliers(test_data_key, test_data, |values| {
                    let samples = Samples::from(values);
                    DoubleMadOutlierDetector::with_estimator(
                        &samples,
                        SimpleNormalizedEstimator::default(),
                    )
                })
            };

            if test_data.values.is_empty() {
                assert!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(action)).is_err(),
                    "Expected panic for test case: {}",
                    test_data_key
                );
            } else {
                action();
            }
        }
    }

    #[test]
    fn double_mad_outlier_detector_hd_qe_test() {
        let test_data_map = hd_qe_test_data_map();

        for (test_data_key, test_data) in test_data_map.iter() {
            let action = || {
                check_outliers(test_data_key, test_data, |values| {
                    let samples = Samples::from(values);
                    DoubleMadOutlierDetector::with_estimator(
                        &samples,
                        HarrellDavisNormalizedEstimator,
                    )
                })
            };

            if test_data.values.is_empty() {
                assert!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(action)).is_err(),
                    "Expected panic for test case: {}",
                    test_data_key
                );
            } else {
                action();
            }
        }
    }
}
