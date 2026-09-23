mod model_parser;
mod model_spec_parser;
mod spec_parser;
mod transform_parser;
mod transform_spec_parser;
mod utils;

pub use model_parser::*;
pub use spec_parser::*;
pub use transform_parser::*;

pub use utils::*;

#[derive(Debug, Clone, PartialEq, Eq, strum::EnumString, strum::Display)]
#[strum(ascii_case_insensitive)]
pub enum ForecastModelKind {
    Adida,
    Arima,
    AutoArima,
    Croston,
    Garch,
    Holt,
    Imapa,
    Sarima,
    SeasonalEs,
    SeasonalNaive,
    Ses,
    Sma,
    Tbats,
    AutoTbats,
    Theta,
    Tsb,
    Mstl,
    Mfles,
    Naive,
    Ets,
    AutoEts,
    HoltWinters,
    RandomWalkWithDrift,
}

#[derive(Debug, Clone, PartialEq, Eq, strum::EnumString, strum::Display)]
#[strum(ascii_case_insensitive)]
pub enum ForecastTransformKind {
    Difference,
    SeasonalDifference,
    BoxCox,
    YeoJohnson,
    Scale,
    Log,
}
