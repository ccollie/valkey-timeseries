
#[derive(Debug, Clone, Copy)]
pub struct ConfidenceInterval {
    pub estimation: f64,
    pub lower: f64,
    pub upper: f64,
    pub confidence_level: f64,
}