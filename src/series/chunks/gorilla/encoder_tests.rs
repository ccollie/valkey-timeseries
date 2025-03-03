#[cfg(test)]
mod tests {
    use crate::common::time::current_time_millis;
    use crate::common::Sample;
    use crate::series::chunks::gorilla::GorillaEncoder;
    use crate::tests::generators::{DataGenerator, RandAlgo};
    use std::time::Duration;

    #[test]
    fn test_gorilla_encoder_encode_decode() {
        let tests = vec![
            ("one data point", vec![Sample::new(1600000000, 0.1)], false),
            (
                "data points at regular intervals",
                vec![
                    Sample::new(1600000000, 0.1),
                    Sample::new(1600000060, 0.1),
                    Sample::new(1600000120, 0.1),
                    Sample::new(1600000180, 0.1),
                ],
                false,
            ),
            (
                "data points at random intervals",
                vec![
                    Sample::new(1600000000, 0.1),
                    Sample::new(1600000060, 1.1),
                    Sample::new(1600000182, 15.01),
                    Sample::new(1600000400, 0.01),
                    Sample::new(1600002000, 10.8),
                ],
                false,
            ), 
        ];

        for (_name, input, want_err) in tests {
            let mut encoder = GorillaEncoder::new();
            for point in &input {
                encoder.add_sample(point).unwrap();
            }

            let buf = encoder.buf();

            let got = encoder.iter().collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(input, got);
        }
    }

    const ONE_DAY: Duration = Duration::from_secs(86400);

    #[test]
    fn test_gorilla_encoder_encode_decode_many() {
        let now = current_time_millis();
        let start = now.saturating_sub((4 * ONE_DAY).as_millis() as i64);
        let data = DataGenerator::builder()
            .start(start)
            .algorithm(RandAlgo::MackeyGlass)
            .samples(5000)
            .build()
            .generate();

        let mut encoder = GorillaEncoder::new();
        for sample in data.iter() {
            encoder.add_sample(sample).unwrap();
        }

        let got = encoder.iter().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(data, got);
    }
}
