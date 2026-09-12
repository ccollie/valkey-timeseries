use super::ForecastTransformKind;
use super::SpecValue;
use super::transform_spec_parser::{TransformSpec, TransformSpecError, parse_transform_specs};
use crate::analysis::forecasting::DynTransform;
use anofox_forecast::models::BoxedForecaster;
use anofox_forecast::transform::transforms::{
    BoxCoxTransform, DifferenceTransform, LogTransform, ScaleMethod, ScaleTransform,
    SeasonalDifferenceTransform, YeoJohnsonTransform,
};
use anofox_forecast::transform::{Pipeline, Transform};

pub fn build_transforms_from_specs(
    input: &str,
) -> Result<Vec<Box<dyn Transform>>, TransformSpecError> {
    parse_transform_specs(input)?
        .into_iter()
        .map(build_single_transform)
        .collect()
}

/// Wrap `model` in a [`Pipeline`] that applies `transforms` before fitting and
/// inverts them on the way out, so fitted values, predictions and intervals
/// all come back in the original units.
///
/// Transforms are stateful (`fit_transform` learns parameters), so each model
/// gets its own clone of the chain. With no transforms the model is returned
/// untouched rather than wrapped in a no-op pipeline.
pub fn wrap_model_with_transforms(
    model: BoxedForecaster,
    transforms: &[Box<dyn Transform>],
) -> BoxedForecaster {
    if transforms.is_empty() {
        return model;
    }
    let mut builder = Pipeline::builder().model(model);
    for transform in transforms {
        builder = builder.transform(DynTransform::new(transform.clone()));
    }
    Box::new(builder.build())
}

pub fn build_single_transform(
    mut spec: TransformSpec,
) -> Result<Box<dyn Transform>, TransformSpecError> {
    match spec.transform_type {
        ForecastTransformKind::Difference => {
            spec.ensure_arity(1)?;
            let d = as_usize(&spec.positional_args[0], &spec.transform_name)?;
            Ok(Box::new(DifferenceTransform::new(d)))
        }
        ForecastTransformKind::SeasonalDifference => {
            spec.ensure_arity(1)?;
            let period = as_usize(&spec.positional_args[0], &spec.transform_name)?;
            Ok(Box::new(SeasonalDifferenceTransform::new(period)))
        }
        ForecastTransformKind::BoxCox => {
            let positional_lambda = match spec.positional_args.as_slice() {
                [] => None,
                [value] => Some(parse_lambda(value, &spec.transform_name)?),
                _ => {
                    return Err(TransformSpecError::new(format!(
                        "Transform {} expects at most one positional lambda argument",
                        spec.transform_name
                    )));
                }
            };

            let keyword_lambda = remove_kwarg(&mut spec, "lambda")
                .map(|value| parse_lambda(&value, &spec.transform_name))
                .transpose()?;

            if !spec.keyword_args.is_empty() {
                let unsupported = spec
                    .keyword_args
                    .iter()
                    .map(|(key, _)| key.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(TransformSpecError::new(format!(
                    "Unsupported keyword argument(s) for transform {}: {}",
                    spec.transform_name, unsupported
                )));
            }

            if positional_lambda.is_some() && keyword_lambda.is_some() {
                return Err(TransformSpecError::new(format!(
                    "Transform {} received lambda both positionally and as keyword",
                    spec.transform_name
                )));
            }

            let lambda = positional_lambda.or(keyword_lambda);
            Ok(match lambda {
                Some(lambda) => Box::new(BoxCoxTransform::with_lambda(lambda)),
                None => Box::new(BoxCoxTransform::auto()),
            })
        }
        ForecastTransformKind::YeoJohnson => {
            spec.ensure_arity(0)?;
            Ok(Box::new(YeoJohnsonTransform::auto()))
        }
        ForecastTransformKind::Scale => {
            spec.ensure_arity(1)?;
            let method = parse_scale_method(&spec.positional_args[0], &spec.transform_name)?;
            Ok(Box::new(ScaleTransform::new(method)))
        }
        ForecastTransformKind::Log => {
            spec.ensure_arity(0)?;
            Ok(Box::new(LogTransform::new()))
        }
    }
}

fn as_usize(value: &SpecValue, transform_name: &str) -> Result<usize, TransformSpecError> {
    value.as_usize().map_err(|_| {
        TransformSpecError::new(format!(
            "Transform {transform_name} expects a non-negative integer positional argument"
        ))
    })
}

fn parse_lambda(value: &SpecValue, transform_name: &str) -> Result<f64, TransformSpecError> {
    value.as_float().map_err(|_| {
        TransformSpecError::new(format!(
            "Transform {transform_name} expects lambda to be a numeric value"
        ))
    })
}

fn remove_kwarg(spec: &mut TransformSpec, key: &str) -> Option<SpecValue> {
    if let Some(pos) = spec.keyword_args.iter().position(|(k, _)| k == key) {
        Some(spec.keyword_args.remove(pos).1)
    } else {
        None
    }
}

fn parse_scale_method(
    value: &SpecValue,
    transform_name: &str,
) -> Result<ScaleMethod, TransformSpecError> {
    let ident = value.as_ident().ok_or_else(|| {
        TransformSpecError::new(format!(
            "Transform {transform_name} expects scale method as identifier or string"
        ))
    })?;

    if ident.eq_ignore_ascii_case("standardize") {
        Ok(ScaleMethod::Standardize)
    } else if ident.eq_ignore_ascii_case("normalize") {
        Ok(ScaleMethod::Normalize)
    } else if ident.eq_ignore_ascii_case("robustscale") || ident.eq_ignore_ascii_case("robust") {
        Ok(ScaleMethod::RobustScale)
    } else {
        Err(TransformSpecError::new(format!(
            "Unsupported Scale method '{ident}', expected one of Standardize, Normalize, RobustScale"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::build_transforms_from_specs;
    use super::wrap_model_with_transforms;
    use anofox_forecast::core::TimeSeries;
    use anofox_forecast::models::{BoxedForecaster, baseline::Naive};
    use chrono::{TimeZone, Utc};

    #[test]
    fn builds_supported_transforms_from_specs() {
        let transforms = build_transforms_from_specs(
            "Difference(1), SeasonalDifference(12), BoxCox, YeoJohnson, Scale(Standardize), Log",
        )
        .unwrap();

        assert_eq!(transforms.len(), 6);
    }

    #[test]
    fn supports_boxcox_lambda_positional_and_keyword() {
        let transforms = build_transforms_from_specs("BoxCox(0.25), BoxCox(lambda=0.5)").unwrap();
        assert_eq!(transforms.len(), 2);
    }

    #[test]
    fn wrap_model_with_transforms_inverts_predictions() {
        // A linear trend: after Difference(1) it is constant, so a Naive model
        // on the differenced data forecasts the slope, and the inverse yields
        // a continued line in the original units.
        let ts: Vec<_> = (0..20)
            .map(|i| Utc.timestamp_opt(i * 60, 0).unwrap())
            .collect();
        let values: Vec<f64> = (0..20).map(|i| 10.0 + 2.0 * i as f64).collect();
        let series = TimeSeries::univariate(ts, values).unwrap();

        let transforms = build_transforms_from_specs("Difference(1)").unwrap();
        let mut model = wrap_model_with_transforms(Box::new(Naive::new()), &transforms);
        let forecast = model.fit_predict(&series, 3).unwrap();
        let predicted = forecast.primary();
        assert_eq!(predicted.len(), 3);
        for (i, v) in predicted.iter().enumerate() {
            let expected = 10.0 + 2.0 * (20 + i) as f64;
            assert!((v - expected).abs() < 1e-9, "point {i}: {v} != {expected}");
        }
    }

    #[test]
    fn fitted_values_survive_model_warmup_nans_through_differencing() {

        // Naive's first fitted value on the differenced series is undefined.
        // Without the DynTransform fix-up the cumulative inverse turns that
        // one NaN into an all-NaN fitted series and metrics cannot be scored.
        let n = 30;
        let ts: Vec<_> = (0..n)
            .map(|i| Utc.timestamp_opt(i * 60, 0).unwrap())
            .collect();
        let values: Vec<f64> = (0..n).map(|i| 1.0 + 2.0 * i as f64).collect();
        let series = TimeSeries::univariate(ts, values.clone()).unwrap();

        let transforms = build_transforms_from_specs("Difference(1)").unwrap();
        let mut model = wrap_model_with_transforms(Box::new(Naive::new()), &transforms);
        model.fit_predict(&series, 1).unwrap();
        let fitted = model
            .fitted_values()
            .expect("pipeline exposes fitted values");
        assert_eq!(fitted.len(), values.len());

        let warmup = fitted.iter().take_while(|v| !v.is_finite()).count();
        assert!(warmup >= 1 && warmup < fitted.len(), "warmup = {warmup}");
        // Only the warm-up prefix may be NaN, and every value after it must
        // sit on the original line (differenced Naive fits a line exactly).
        for (i, (f, a)) in fitted.iter().zip(&values).enumerate().skip(warmup) {
            assert!(f.is_finite(), "fitted[{i}] is not finite");
            assert!((f - a).abs() < 1e-9, "fitted[{i}] = {f}, actual = {a}");
        }
    }

    #[test]
    fn wrap_model_without_transforms_is_identity() {
        let inner: BoxedForecaster = Box::new(Naive::new());
        let expected = inner.name().to_string();
        let model = wrap_model_with_transforms(inner, &[]);
        assert_eq!(model.name(), expected);
    }

    #[test]
    fn rejects_boxcox_duplicate_lambda_sources() {
        let err = build_transforms_from_specs("BoxCox(0.25, lambda=0.5)").unwrap_err();
        assert!(
            err.to_string()
                .contains("received lambda both positionally and as keyword")
        );
    }
}
