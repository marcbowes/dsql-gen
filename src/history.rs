//! History tracking with timestamps
//!
//! This module provides data structures and utilities for tracking historical data
//! with explicit timestamps, replacing the previous vec-based approach that relied
//! on UI tick rates.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// A data point with an associated timestamp
#[derive(Clone, Debug)]
pub struct TimestampedDataPoint<T> {
    pub timestamp: Instant,
    pub value: T,
}

impl<T> TimestampedDataPoint<T> {
    /// Create a new timestamped data point with the current time
    pub fn new(value: T) -> Self {
        Self {
            timestamp: Instant::now(),
            value,
        }
    }

    /// Create a new timestamped data point with a specific timestamp
    pub fn with_timestamp(value: T, timestamp: Instant) -> Self {
        Self { timestamp, value }
    }
}

/// A history tracker that maintains timestamped data points within a time window
#[derive(Clone, Debug)]
pub struct TimestampedHistory<T> {
    data: VecDeque<TimestampedDataPoint<T>>,
    pub window_duration: Duration,
}

impl<T> TimestampedHistory<T> {
    /// Create a new timestamped history with the specified time window
    pub fn new(window_duration: Duration) -> Self {
        Self {
            data: VecDeque::new(),
            window_duration,
        }
    }

    /// Add a new data point with the current timestamp
    pub fn push(&mut self, value: T) {
        self.push_with_timestamp(value, Instant::now());
    }

    /// Add a new data point with a specific timestamp
    pub fn push_with_timestamp(&mut self, value: T, timestamp: Instant) {
        self.data
            .push_back(TimestampedDataPoint { timestamp, value });
        self.cleanup_old_data(timestamp);
    }

    /// Remove data points older than the window duration
    fn cleanup_old_data(&mut self, current_time: Instant) {
        let cutoff_time = current_time
            .checked_sub(self.window_duration)
            .unwrap_or(current_time);

        while let Some(front) = self.data.front() {
            if front.timestamp < cutoff_time {
                self.data.pop_front();
            } else {
                break;
            }
        }
    }

    /// Get all data points in the history
    pub fn data(&self) -> &VecDeque<TimestampedDataPoint<T>> {
        &self.data
    }

    /// Get the number of data points
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the history is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Clear all data points
    pub fn clear(&mut self) {
        self.data.clear();
    }
}

/// Utility functions for bucketing timestamped data for chart rendering
pub mod bucketing {
    use super::*;

    /// Bucket configuration for chart rendering
    #[derive(Clone, Debug)]
    pub struct BucketConfig {
        pub bucket_duration: Duration,
        pub total_buckets: usize,
    }

    impl BucketConfig {
        /// Create a new bucket configuration
        pub fn new(bucket_duration: Duration, total_buckets: usize) -> Self {
            Self {
                bucket_duration,
                total_buckets,
            }
        }
    }

    /// Bucket timestamped data points for chart rendering
    pub fn bucket_data<T, F>(
        data: &VecDeque<TimestampedDataPoint<T>>,
        config: BucketConfig,
        aggregator: F,
        default_value: f64,
    ) -> Vec<f64>
    where
        F: Fn(&[&T]) -> f64,
    {
        let mut buckets = vec![default_value; config.total_buckets];

        if data.is_empty() {
            return buckets;
        }

        let now = Instant::now();
        let total_duration = config.bucket_duration * config.total_buckets as u32;
        let start_time = now.checked_sub(total_duration).unwrap_or(now);

        // Group data points by bucket
        let mut bucket_data: Vec<Vec<&T>> = vec![Vec::new(); config.total_buckets];

        for point in data {
            if point.timestamp >= start_time {
                let elapsed = point.timestamp.duration_since(start_time);
                let bucket_index =
                    (elapsed.as_secs_f64() / config.bucket_duration.as_secs_f64()) as usize;

                if bucket_index < config.total_buckets {
                    bucket_data[bucket_index].push(&point.value);
                }
            }
        }

        // Aggregate data in each bucket
        for (i, bucket_values) in bucket_data.iter().enumerate() {
            if !bucket_values.is_empty() {
                buckets[i] = aggregator(bucket_values);
            }
        }

        buckets
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_timestamped_data_point() {
        let point = TimestampedDataPoint::new(42.0);
        assert_eq!(point.value, 42.0);
    }

    #[test]
    fn test_timestamped_history() {
        let mut history = TimestampedHistory::new(Duration::from_secs(1));

        history.push(1.0);
        history.push(2.0);

        assert_eq!(history.len(), 2);
        assert!(!history.is_empty());

        // Test cleanup by waiting and adding old data
        thread::sleep(Duration::from_millis(1100));
        history.push(3.0);

        // Should have cleaned up old data
        assert!(history.len() <= 2);
    }

    #[test]
    fn test_bucketing() {
        let mut data = VecDeque::new();
        let now = Instant::now();

        // Add some test data
        data.push_back(TimestampedDataPoint::with_timestamp(
            1.0,
            now - Duration::from_secs(5),
        ));
        data.push_back(TimestampedDataPoint::with_timestamp(
            2.0,
            now - Duration::from_secs(3),
        ));
        data.push_back(TimestampedDataPoint::with_timestamp(
            3.0,
            now - Duration::from_secs(1),
        ));

        let config = bucketing::BucketConfig::new(Duration::from_secs(2), 5);
        let buckets = bucketing::bucket_data(
            &data,
            config,
            |values| values.iter().map(|&&v| v).sum::<f64>() / values.len() as f64,
            0.0,
        );

        assert_eq!(buckets.len(), 5);
    }
}
