use super::response_generated::{
    PostingStat as FBPostingStat, PostingStatArgs, PostingStats as FBPostingStats, PostingStatsArgs,
};
use crate::commands::DEFAULT_STATS_RESULTS_LIMIT;
use crate::error_consts;
use crate::fanout::request::common::load_flatbuffers_object;
use crate::fanout::request::ValkeyResult;
use crate::fanout::serialization::{Deserialized, Serialized};
use crate::fanout::{CommandMessageType, MultiShardCommand, TrackerEnum};
use crate::series::index::{with_timeseries_index, PostingStat, PostingsStats};
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, Vector, WIPOffset};
use valkey_module::{Context, ValkeyError};

pub struct StatsCommand;

impl MultiShardCommand for StatsCommand {
    type REQ = StatsRequest;
    type RES = PostingsStats;
    type STATE = u64;

    fn request_type() -> CommandMessageType {
        CommandMessageType::Stats
    }

    fn exec(ctx: &Context, req: Self::REQ) -> ValkeyResult<Self::RES> {
        let limit = req.limit;
        Ok(with_timeseries_index(ctx, |index| index.stats("", limit)))
    }

    fn update_tracker(tracker: &TrackerEnum, res: Self::RES) {
        if let TrackerEnum::Stats(t) = tracker {
            t.update(res);
        } else {
            panic!("BUG: Invalid tracker type");
        }
    }
}

pub struct StatsRequest {
    pub limit: usize,
}

impl Default for StatsRequest {
    fn default() -> Self {
        StatsRequest {
            limit: DEFAULT_STATS_RESULTS_LIMIT,
        }
    }
}

impl Serialized for StatsRequest {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let limit: u64 = self.limit as u64; // todo: uvarint encoding
        buf.extend_from_slice(limit.to_le_bytes().as_slice());
    }
}

impl Deserialized for StatsRequest {
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        const SIZE: usize = size_of::<u64>();
        // todo: uvarint encoding
        if buf.len() != SIZE {
            return Err(ValkeyError::Str(
                error_consts::COMMAND_DESERIALIZATION_ERROR,
            ));
        }
        let limit_buf: Option<[u8; SIZE]> = buf[0..SIZE].try_into().ok();
        let Some(limit_bytes) = limit_buf else {
            return Err(ValkeyError::Str(
                error_consts::COMMAND_DESERIALIZATION_ERROR,
            ));
        };
        let limit = u64::from_le_bytes(limit_bytes);

        Ok(StatsRequest {
            limit: limit as usize,
        })
    }
}

impl Serialized for PostingsStats {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let mut bldr = FlatBufferBuilder::with_capacity(512);
        let mut dest: Vec<WIPOffset<FBPostingStat<'_>>> = Vec::with_capacity(10);

        serialize_posting_stat_list(&mut bldr, &mut dest, &self.cardinality_metrics_stats);
        let cardinality_metrics_stats = bldr.create_vector(&dest);

        serialize_posting_stat_list(&mut bldr, &mut dest, &self.cardinality_label_stats);
        let cardinality_label_stats = bldr.create_vector(&dest);

        serialize_posting_stat_list(&mut bldr, &mut dest, &self.label_value_stats);
        let label_value_stats = bldr.create_vector(&dest);

        serialize_posting_stat_list(&mut bldr, &mut dest, &self.label_value_pairs_stats);
        let label_value_pairs_stats = bldr.create_vector(&dest);

        let obj = FBPostingStats::create(
            &mut bldr,
            &PostingStatsArgs {
                cardinality_metrics_stats: Some(cardinality_metrics_stats),
                cardinality_label_stats: Some(cardinality_label_stats),
                label_value_pairs_stats: Some(label_value_pairs_stats),
                label_value_stats: Some(label_value_stats),
                num_label_pairs: self.num_label_pairs as u64,
                num_labels: self.num_labels as u64,
                series_count: self.series_count,
            },
        );

        bldr.finish(obj, None);
        let data = bldr.finished_data();
        buf.extend_from_slice(data);
    }
}

impl Deserialized for PostingsStats {
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        let root = load_flatbuffers_object::<FBPostingStats>(buf, "PostingsStats")?;
        let cardinality_metrics_stats =
            deserialize_posting_stat_list(root.cardinality_metrics_stats());
        let cardinality_label_stats = deserialize_posting_stat_list(root.cardinality_label_stats());
        let label_value_pairs_stats = deserialize_posting_stat_list(root.label_value_pairs_stats());
        let label_value_stats = deserialize_posting_stat_list(root.label_value_stats());
        let series_count = root.series_count();

        Ok(PostingsStats {
            cardinality_metrics_stats,
            cardinality_label_stats,
            label_value_pairs_stats,
            label_value_stats,
            num_label_pairs: root.num_label_pairs() as usize,
            num_labels: root.num_labels() as usize,
            series_count,
        })
    }
}

fn serialize_posting_stat_list<'a>(
    bldr: &mut FlatBufferBuilder<'a>,
    dest: &mut Vec<WIPOffset<FBPostingStat<'a>>>,
    items: &[PostingStat],
) {
    dest.clear();
    for item in items.iter() {
        let name = bldr.create_string(&item.name);
        dest.push(FBPostingStat::create(
            bldr,
            &PostingStatArgs {
                name: Some(name),
                count: item.count,
            },
        ));
    }
}

type SerializedStatList<'a> = Vector<'a, ForwardsUOffset<FBPostingStat<'a>>>;
fn deserialize_posting_stat_list(items: Option<SerializedStatList>) -> Vec<PostingStat> {
    items
        .unwrap_or_default()
        .iter()
        .map(|x| PostingStat {
            name: x.name().unwrap().to_string(),
            count: x.count(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::series::index::{PostingStat, PostingsStats};

    #[test]
    fn test_postingstats_serialization_deserialization() {
        // Create a sample PostingsStats instance with test data
        let stats = PostingsStats {
            cardinality_metrics_stats: vec![
                PostingStat {
                    name: "metric1".to_string(),
                    count: 100,
                },
                PostingStat {
                    name: "metric2".to_string(),
                    count: 200,
                },
            ],
            cardinality_label_stats: vec![
                PostingStat {
                    name: "label1".to_string(),
                    count: 50,
                },
                PostingStat {
                    name: "label2".to_string(),
                    count: 75,
                },
            ],
            label_value_stats: vec![
                PostingStat {
                    name: "value1".to_string(),
                    count: 30,
                },
                PostingStat {
                    name: "value2".to_string(),
                    count: 40,
                },
            ],
            label_value_pairs_stats: vec![
                PostingStat {
                    name: "pair1".to_string(),
                    count: 25,
                },
                PostingStat {
                    name: "pair2".to_string(),
                    count: 35,
                },
            ],
            num_label_pairs: 60,
            num_labels: 125,
            series_count: 1000,
        };

        // Serialize the stats
        let mut buf = Vec::new();
        stats.serialize(&mut buf);

        // Ensure we got some data
        assert!(!buf.is_empty(), "Serialized buffer should not be empty");

        // Deserialize back into an object
        let deserialized =
            PostingsStats::deserialize(&buf).expect("Deserialization should succeed");

        // Verify that all fields match
        assert_eq!(stats.num_label_pairs, deserialized.num_label_pairs);
        assert_eq!(stats.num_labels, deserialized.num_labels);

        // Check cardinality_metrics_stats
        assert_eq!(
            stats.cardinality_metrics_stats.len(),
            deserialized.cardinality_metrics_stats.len()
        );
        for (original, deserialized_stat) in stats
            .cardinality_metrics_stats
            .iter()
            .zip(deserialized.cardinality_metrics_stats.iter())
        {
            assert_eq!(original.name, deserialized_stat.name);
            assert_eq!(original.count, deserialized_stat.count);
        }

        // Check cardinality_label_stats
        assert_eq!(
            stats.cardinality_label_stats.len(),
            deserialized.cardinality_label_stats.len()
        );
        for (original, deserialized_stat) in stats
            .cardinality_label_stats
            .iter()
            .zip(deserialized.cardinality_label_stats.iter())
        {
            assert_eq!(original.name, deserialized_stat.name);
            assert_eq!(original.count, deserialized_stat.count);
        }

        // Check label_value_stats
        assert_eq!(
            stats.label_value_stats.len(),
            deserialized.label_value_stats.len()
        );
        for (original, deserialized_stat) in stats
            .label_value_stats
            .iter()
            .zip(deserialized.label_value_stats.iter())
        {
            assert_eq!(original.name, deserialized_stat.name);
            assert_eq!(original.count, deserialized_stat.count);
        }

        // Check label_value_pairs_stats
        assert_eq!(
            stats.label_value_pairs_stats.len(),
            deserialized.label_value_pairs_stats.len()
        );
        for (original, deserialized_stat) in stats
            .label_value_pairs_stats
            .iter()
            .zip(deserialized.label_value_pairs_stats.iter())
        {
            assert_eq!(original.name, deserialized_stat.name);
            assert_eq!(original.count, deserialized_stat.count);
        }
        // Check series_count
        assert_eq!(stats.series_count, deserialized.series_count);
    }

    #[test]
    fn test_empty_postingstats() {
        // Test with empty lists to ensure edge cases work
        let empty_stats = PostingsStats {
            cardinality_metrics_stats: vec![],
            cardinality_label_stats: vec![],
            label_value_stats: vec![],
            label_value_pairs_stats: vec![],
            num_label_pairs: 0,
            num_labels: 0,
            series_count: 0,
        };

        // Serialize and deserialize
        let mut buf = Vec::new();
        empty_stats.serialize(&mut buf);
        let deserialized =
            PostingsStats::deserialize(&buf).expect("Deserialization should succeed");

        // Verify empty lists are properly handled
        assert_eq!(deserialized.cardinality_metrics_stats.len(), 0);
        assert_eq!(deserialized.cardinality_label_stats.len(), 0);
        assert_eq!(deserialized.label_value_stats.len(), 0);
        assert_eq!(deserialized.label_value_pairs_stats.len(), 0);
        assert_eq!(deserialized.num_label_pairs, 0);
        assert_eq!(deserialized.num_labels, 0);
        assert_eq!(deserialized.series_count, 0);
    }

    #[test]
    fn test_large_postingstats() {
        // Create a large PostingStat list to test performance with bigger data
        let large_list: Vec<PostingStat> = (0..1000)
            .map(|i| PostingStat {
                name: format!("metric{}", i),
                count: i as u64,
            })
            .collect();

        let large_stats = PostingsStats {
            cardinality_metrics_stats: large_list.clone(),
            cardinality_label_stats: vec![],
            label_value_stats: vec![],
            label_value_pairs_stats: vec![],
            num_label_pairs: 5000,
            num_labels: 1000,
            series_count: 10000,
        };

        // Serialize and deserialize
        let mut buf = Vec::new();
        large_stats.serialize(&mut buf);
        let deserialized =
            PostingsStats::deserialize(&buf).expect("Deserialization should succeed");

        assert_eq!(deserialized.cardinality_metrics_stats.len(), 1000);
        for (i, stat) in deserialized.cardinality_metrics_stats.iter().enumerate() {
            assert_eq!(stat.name, format!("metric{}", i));
            assert_eq!(stat.count, i as u64);
        }
    }

    #[test]
    fn test_statsrequest_serialization_deserialization() {
        let request = StatsRequest { limit: 123 };

        // Serialize
        let mut buf = Vec::new();
        request.serialize(&mut buf);

        // Deserialize
        let deserialized = StatsRequest::deserialize(&buf).expect("Deserialization should succeed");

        // Verify
        assert_eq!(request.limit, deserialized.limit);
    }
}
