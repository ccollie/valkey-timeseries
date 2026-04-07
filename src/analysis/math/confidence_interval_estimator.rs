use crate::analysis::math::StudentDistribution;

#[derive(Debug, Clone, Copy)]
pub struct ConfidenceInterval {
    pub estimation: f64,
    pub lower: f64,
    pub upper: f64,
    pub confidence_level: f64,
}

pub struct ConfidenceIntervalEstimator {
    pub sample_size: f64,
    pub estimation: f64,
    pub standard_error: f64,
}

impl ConfidenceIntervalEstimator {
    pub fn new(sample_size: f64, estimation: f64, standard_error: f64) -> Self {
        Self {
            sample_size,
            estimation,
            standard_error,
        }
    }

    fn degree_of_freedom(&self) -> f64 {
        self.sample_size - 1.0
    }

    pub fn confidence_interval(&self, confidence_level: f64) -> ConfidenceInterval {
        let dof = self.degree_of_freedom();
        if dof <= 0.0 {
            return ConfidenceInterval {
                estimation: self.estimation,
                lower: f64::NAN,
                upper: f64::NAN,
                confidence_level,
            };
        }
        let margin = self.standard_error * self.z_level(confidence_level);
        ConfidenceInterval {
            estimation: self.estimation,
            lower: self.estimation - margin,
            upper: self.estimation + margin,
            confidence_level,
        }
    }

    pub fn z_level(&self, confidence_level: f64) -> f64 {
        let x = 1.0 - (1.0 - confidence_level) / 2.0;
        let dof = self.degree_of_freedom();
        StudentDistribution::new(dof).quantile(x)
    }
}
