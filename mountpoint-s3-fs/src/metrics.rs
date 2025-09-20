//! Metrics infrastructure
//!
//! This module hooks up the [metrics](https://docs.rs/metrics) facade to a metrics sink that
//! currently just emits them to a tracing log entry.

#[cfg(feature = "otlp_integration")]
pub use crate::metrics_otel::OtlpConfig;
#[cfg(feature = "otlp_integration")]
use crate::metrics_otel::OtlpMetricsExporter;

use std::thread::{self, JoinHandle};
use std::time::Duration;

use dashmap::DashMap;
use metrics::{Key, Metadata, Recorder, Unit};
use sysinfo::{MemoryRefreshKind, ProcessRefreshKind, ProcessesToUpdate, System, get_current_pid};

use crate::sync::Arc;
use crate::sync::mpsc::{RecvTimeoutError, Sender, channel};

mod data;
pub use data::MetricValue;
use data::*;

mod tracing_span;
pub use tracing_span::metrics_tracing_span_layer;

/// How long between drains of each thread's local metrics into the global sink
const AGGREGATION_PERIOD: Duration = Duration::from_secs(5);

/// The log target to use for emitted metrics
pub const TARGET_NAME: &str = "mountpoint_s3_fs::metrics";

/// Configuration for metrics collection
pub enum MetricsConfig {
    /// OpenTelemetry configuration (only available with "otlp_integration" feature)
    #[cfg(feature = "otlp_integration")]
    Otlp(OtlpConfig),
}

/// Initialize and install the global metrics sink, and return a handle that can be used to shut
/// the sink down. The sink should only be shut down after any threads that generate metrics are
/// done with their work; metrics generated after shutting down the sink will be lost.
///
/// Panics if a sink has already been installed.
pub fn install(config: Option<MetricsConfig>) -> anyhow::Result<MetricsSinkHandle> {
    let sink = Arc::new(MetricsSink::new(config)?);
    let mut sys = System::new();

    let (tx, rx) = channel();

    let publisher_thread = {
        let inner = Arc::clone(&sink);
        thread::spawn(move || {
            loop {
                match rx.recv_timeout(AGGREGATION_PERIOD) {
                    Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
                    Err(RecvTimeoutError::Timeout) => {
                        poll_process_metrics(&mut sys);
                        inner.publish()
                    }
                }
            }
            // Drain metrics one more time before shutting down. This has a chance of missing
            // any new metrics data after the sink shuts down, but we assume a clean shutdown
            // stops generating new metrics before shutting down the sink.
            poll_process_metrics(&mut sys);
            inner.publish();
        })
    };

    let handle = MetricsSinkHandle {
        shutdown: tx,
        handle: Some(publisher_thread),
    };

    let recorder = MetricsRecorder { sink };
    metrics::set_global_recorder(recorder)
        .map_err(|e| anyhow::anyhow!("Failed to set global metrics recorder: {}", e))?;

    Ok(handle)
}

/// Report process level metrics
fn poll_process_metrics(sys: &mut System) {
    if let Ok(pid) = get_current_pid() {
        let last_mem = sys.process(pid).map_or(0, |process| process.memory());
        sys.refresh_memory_specifics(MemoryRefreshKind::nothing().with_ram());
        sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            false,
            ProcessRefreshKind::nothing().with_memory(),
        );
        if let Some(process) = sys.process(pid) {
            // update the metrics only when there is some change, otherwise it will be too spammy.
            if last_mem != process.memory() {
                metrics::gauge!("process.memory_usage").set(process.memory() as f64);
                metrics::gauge!("system.available_memory").set(sys.available_memory() as f64);
            }
        }
    }
}

#[cfg(feature = "otlp_integration")]
static OTLP_METRICS_FILTER: &[&str] = &[
    "fuse.op_latency_us",
    "fuse.io_size", 
    "fuse.op_failures",
    "fuse.mp_workers.total_count",
    "fuse.mp_workers.idle_count",
    "fuse.mp_workers.busy_count",
    "s3.requests",
    "s3.requests.failures", 
    "s3.requests.total_latency_us",
    "s3.requests.canceled",
    "process.memory_usage",
    "system.available_memory",
];

#[cfg(feature = "otlp_integration")]
static OTLP_EXPERIMENTAL_METRICS_FILTER: &[&str] = &[
    "s3.meta_requests",
    "s3.meta_requests.failures",
    "s3.meta_requests.total_latency_us", 
    "s3.meta_requests.first_byte_latency_us",
];

#[cfg(feature = "otlp_integration")]
fn should_export_to_otlp(metric_name: &str) -> bool {
    OTLP_METRICS_FILTER.contains(&metric_name) || OTLP_EXPERIMENTAL_METRICS_FILTER.contains(&metric_name)
}

#[derive(Debug)]
struct MetricEntry {
    metric: Metric,
    unit: Option<Unit>,
}

#[derive(Debug)]
struct MetricsSink {
    metrics: DashMap<Key, MetricEntry>,
    #[cfg(feature = "otlp_integration")]
    otlp_exporter: Option<OtlpMetricsExporter>,
}

impl MetricsSink {
    fn new(config: Option<MetricsConfig>) -> anyhow::Result<Self> {
        // Match on the config to determine what kind of metrics sink to create
        match config {
            None => Ok(Self {
                metrics: DashMap::with_capacity(64),
                #[cfg(feature = "otlp_integration")]
                otlp_exporter: None,
            }),

            // OTLP configuration (only available with "otlp_integration" feature)
            #[cfg(feature = "otlp_integration")]
            Some(MetricsConfig::Otlp(config)) => {
                // Basic validation of the endpoint URL
                if !config.endpoint.starts_with("http://") && !config.endpoint.starts_with("https://") {
                    return Err(anyhow::anyhow!(
                        "Invalid OTLP endpoint configuration: endpoint must start with http:// or https://"
                    ));
                }

                // Use TryInto to convert OtlpConfig to OtlpMetricsExporter to validate configuration
                match std::convert::TryInto::<OtlpMetricsExporter>::try_into(&config) {
                    Ok(exporter) => {
                        tracing::info!("OpenTelemetry metrics export enabled to {}", config.endpoint);
                        Ok(Self {
                            metrics: DashMap::with_capacity(64),
                            otlp_exporter: Some(exporter),
                        })
                    }
                    Err(e) => {
                        tracing::error!("Failed to initialise OTLP exporter: {}", e);
                        Err(anyhow::anyhow!(
                            "Failed to initialize OTLP metrics exporter: {}. If metrics export is not required, omit the OTLP configuration.",
                            e
                        ))
                    }
                }
            }
        }
    }

    fn counter(&self, key: &Key, unit: Option<metrics::Unit>) -> metrics::Counter {
        let entry = self.metrics.entry(key.clone()).or_insert_with(|| {
            #[cfg(feature = "otlp_integration")]
            if let Some(exporter) = &self.otlp_exporter {
                if should_export_to_otlp(key.name()) {
                    let unit_str = unit
                        .and_then(|u| crate::metrics_otel::convert_unit_to_otlp(Some(u)))
                        .map(String::from);
                    let otlp_counter = exporter.create_counter_instrument(key.name().to_string(), unit_str);
                    let attributes: Vec<opentelemetry::KeyValue> = key
                        .labels()
                        .map(|label| opentelemetry::KeyValue::new(label.key().to_string(), label.value().to_string()))
                        .collect();
                    return MetricEntry {
                        metric: Metric::Counter(Arc::new(data::ValueAndCount::with_otlp(otlp_counter, attributes))),
                        unit,
                    };
                }
            }
            MetricEntry {
                metric: Metric::counter(),
                unit,
            }
        });
        entry.metric.as_counter()
    }

    fn gauge(&self, key: &Key, unit: Option<metrics::Unit>) -> metrics::Gauge {
        let entry = self.metrics.entry(key.clone()).or_insert_with(|| {
            #[cfg(feature = "otlp_integration")]
            if let Some(exporter) = &self.otlp_exporter {
                if should_export_to_otlp(key.name()) {
                    let unit_str = unit
                        .and_then(|u| crate::metrics_otel::convert_unit_to_otlp(Some(u)))
                        .map(String::from);
                    let otlp_gauge = exporter.create_gauge_instrument(key.name().to_string(), unit_str);
                    let attributes: Vec<opentelemetry::KeyValue> = key
                        .labels()
                        .map(|label| opentelemetry::KeyValue::new(label.key().to_string(), label.value().to_string()))
                        .collect();
                    return MetricEntry {
                        metric: Metric::Gauge(Arc::new(data::AtomicGauge::with_otlp(otlp_gauge, attributes))),
                        unit,
                    };
                }
            }
            MetricEntry {
                metric: Metric::gauge(),
                unit,
            }
        });
        entry.metric.as_gauge()
    }

    fn histogram(&self, key: &Key, unit: Option<metrics::Unit>) -> metrics::Histogram {
        let entry = self.metrics.entry(key.clone()).or_insert_with(|| {
            #[cfg(feature = "otlp_integration")]
            if let Some(exporter) = &self.otlp_exporter {
                if should_export_to_otlp(key.name()) {
                    let unit_str = unit
                        .and_then(|u| crate::metrics_otel::convert_unit_to_otlp(Some(u)))
                        .map(String::from);
                    let otlp_histogram = exporter.create_histogram_instrument(key.name().to_string(), unit_str);
                    let attributes: Vec<opentelemetry::KeyValue> = key
                        .labels()
                        .map(|label| opentelemetry::KeyValue::new(label.key().to_string(), label.value().to_string()))
                        .collect();
                    return MetricEntry {
                        metric: Metric::Histogram(Arc::new(data::Histogram::with_otlp(otlp_histogram, attributes))),
                        unit,
                    };
                }
            }
            MetricEntry {
                metric: Metric::histogram(),
                unit,
            }
        });
        entry.metric.as_histogram()
    }
}

#[cfg(not(feature = "otlp_integration"))]
impl MetricsSink {
    /// Publish all this sink's metrics to `tracing` log messages
    fn publish(&self) {
        // Collect the output lines so we can sort them to make reading easier
        let mut metrics = vec![];

        for mut entry in self.metrics.iter_mut() {
            let (key, entry) = entry.pair_mut();

            // Get the string representation of the metric (this also resets the metric)
            let Some(metric_str) = entry.metric.fmt_and_reset() else {
                continue;
            };

            let labels = if key.labels().len() == 0 {
                String::new()
            } else {
                format!(
                    "[{}]",
                    key.labels()
                        .map(|label| format!("{}={}", label.key(), label.value()))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            };
            metrics.push(format!("{}{}: {}", key.name(), labels, metric_str));
        }

        metrics.sort();

        for metric in metrics {
            tracing::info!(target: TARGET_NAME, "{}", metric);
        }
    }
}

#[cfg(feature = "otlp_integration")]
impl MetricsSink {
    /// Publish all this sink's metrics to `tracing` log messages
    /// and send metrics to OTLP if enabled
    fn publish(&self) {
        // Collect the output lines so we can sort them to make reading easier
        let mut metrics = vec![];

        for mut entry in self.metrics.iter_mut() {
            let (key, entry) = entry.pair_mut();

            // Get the string representation of the metric (this also resets the metric)
            let Some(metric_str) = entry.metric.fmt_and_reset() else {
                continue;
            };

            let labels = if key.labels().len() == 0 {
                String::new()
            } else {
                format!(
                    "[{}]",
                    key.labels()
                        .map(|label| format!("{}={}", label.key(), label.value()))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            };
            metrics.push(format!("{}{}: {}", key.name(), labels, metric_str));
        }

        metrics.sort();

        for metric in metrics {
            tracing::info!(target: TARGET_NAME, "{}", metric);
        }
    }
}

/// The actual recorder that will be installed for the metrics facade. Just a wrapper around a
/// [MetricsSinkInner] that does all the real work.
struct MetricsRecorder {
    sink: Arc<MetricsSink>,
}

impl Recorder for MetricsRecorder {
    fn describe_counter(
        &self,
        key: metrics::KeyName,
        unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
        let key_obj = Key::from_name(key);
        self.sink.metrics.entry(key_obj).or_insert_with(|| MetricEntry {
            metric: Metric::counter(),
            unit,
        });
    }

    fn describe_gauge(&self, key: metrics::KeyName, unit: Option<metrics::Unit>, _description: metrics::SharedString) {
        let key_obj = Key::from_name(key);
        self.sink.metrics.entry(key_obj).or_insert_with(|| MetricEntry {
            metric: Metric::gauge(),
            unit,
        });
    }

    fn describe_histogram(
        &self,
        key: metrics::KeyName,
        unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
        let key_obj = Key::from_name(key);
        self.sink.metrics.entry(key_obj).or_insert_with(|| MetricEntry {
            metric: Metric::histogram(),
            unit,
        });
    }

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> metrics::Counter {
        let unit = self.sink.metrics.get(key).and_then(|entry| entry.unit);
        self.sink.counter(key, unit)
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> metrics::Gauge {
        let unit = self.sink.metrics.get(key).and_then(|entry| entry.unit);
        self.sink.gauge(key, unit)
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> metrics::Histogram {
        let unit = self.sink.metrics.get(key).and_then(|entry| entry.unit);
        self.sink.histogram(key, unit)
    }
}

#[derive(Debug)]
pub struct MetricsSinkHandle {
    shutdown: Sender<()>,
    handle: Option<JoinHandle<()>>,
}

impl Drop for MetricsSinkHandle {
    fn drop(&mut self) {
        let _ = self.shutdown.send(());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::{Label, with_local_recorder};

    const TEST_COUNTER: &str = "test_counter";
    const TEST_GAUGE: &str = "test_gauge";
    const TEST_HISTOGRAM: &str = "test_histogram";

    #[test]
    fn basic_metrics() {
        let sink = Arc::new(MetricsSink::new(None).unwrap());
        let recorder = MetricsRecorder { sink: sink.clone() };
        with_local_recorder(&recorder, || {
            // Run twice to check reset works
            for _ in 0..2 {
                metrics::counter!(TEST_COUNTER, "type" => "get").increment(1);
                metrics::counter!(TEST_COUNTER, "type" => "put").increment(1);
                metrics::counter!(TEST_COUNTER, "type" => "get").increment(2);
                metrics::counter!(TEST_COUNTER, "type" => "put").increment(2);
                metrics::counter!(TEST_COUNTER, "type" => "get").increment(3);
                metrics::counter!(TEST_COUNTER, "type" => "put").increment(4);

                metrics::gauge!(TEST_GAUGE, "type" => "processing").set(5.0);
                metrics::gauge!(TEST_GAUGE, "type" => "in_queue").set(5.0);
                metrics::gauge!(TEST_GAUGE, "type" => "processing").set(2.0);
                metrics::gauge!(TEST_GAUGE, "type" => "in_queue").set(3.0);

                metrics::histogram!(TEST_HISTOGRAM, "type" => "get").record(3.0);
                metrics::histogram!(TEST_HISTOGRAM, "type" => "put").record(4.0);
                metrics::histogram!(TEST_HISTOGRAM, "type" => "put").record(4.0);

                for mut entry in sink.metrics.iter_mut() {
                    let (key, entry) = entry.pair_mut();
                    assert_eq!(key.labels().count(), 1, "{key} has no labels");
                    match &entry.metric {
                        Metric::Counter(inner) => {
                            assert_eq!(key.name(), TEST_COUNTER);
                            let (sum, n) = inner.load_and_reset().expect("should have a value");
                            assert_eq!(n, 3);
                            let label = key.labels().next().unwrap();
                            if label == &Label::new("type", "get") {
                                assert_eq!(sum, 6);
                            } else if label == &Label::new("type", "put") {
                                assert_eq!(sum, 7);
                            } else {
                                panic!("wrong label");
                            }
                        }
                        Metric::Gauge(inner) => {
                            assert_eq!(key.name(), TEST_GAUGE);
                            let value = inner.load_if_changed();
                            let label = key.labels().next().unwrap();
                            if label == &Label::new("type", "processing") {
                                assert_eq!(value, Some(2.0));
                            } else if label == &Label::new("type", "in_queue") {
                                assert_eq!(value, Some(3.0));
                            } else {
                                panic!("wrong label");
                            }
                        }
                        Metric::Histogram(inner) => {
                            assert_eq!(key.name(), TEST_HISTOGRAM);
                            let label = key.labels().next().unwrap();
                            inner.run_and_reset(|histogram| {
                                if label == &Label::new("type", "get") {
                                    assert_eq!(histogram.len(), 1);
                                    assert_eq!(histogram.count_at(3), 1);
                                } else if label == &Label::new("type", "put") {
                                    assert_eq!(histogram.len(), 2);
                                    assert_eq!(histogram.count_at(4), 2);
                                }
                            });
                        }
                    }
                }
            }

            // Check that each metric is zeroed (returns None) after the end of the loop reset it
            for mut entry in sink.metrics.iter_mut() {
                let entry = entry.value_mut();
                match &entry.metric {
                    Metric::Counter(inner) => assert!(inner.load_and_reset().is_none()),
                    Metric::Gauge(inner) => assert!(inner.load_if_changed().is_none()),
                    Metric::Histogram(inner) => assert!(inner.run_and_reset(|_| panic!("unreachable")).is_none()),
                }
            }

            // Set the gauges to zero and check they emit their change only once
            metrics::gauge!(TEST_GAUGE, "type" => "processing").set(0.0);
            metrics::gauge!(TEST_GAUGE, "type" => "in_queue").set(0.0);
            for mut entry in sink.metrics.iter_mut() {
                let entry = entry.value_mut();
                let Metric::Gauge(inner) = &entry.metric else {
                    continue;
                };
                // We want to emit once to reflect that it's changed to 0, and then not emit again
                assert!(inner.load_if_changed().is_some());
                assert!(inner.load_if_changed().is_none());
            }
        });
    }

    #[test]
    fn units_support() {
        let sink = Arc::new(MetricsSink::new(None).unwrap());
        let recorder = MetricsRecorder { sink: sink.clone() };

        with_local_recorder(&recorder, || {
            metrics::describe_counter!("bytes_counter", Unit::Bytes, "");
            metrics::describe_gauge!("time_gauge", Unit::Milliseconds, "");
            metrics::describe_histogram!("count_histogram", Unit::Count, "");

            metrics::counter!("bytes_counter").increment(1024);
            metrics::gauge!("time_gauge").set(500.0);
            metrics::histogram!("count_histogram").record(5.0);

            for entry in sink.metrics.iter() {
                let (key, entry) = entry.pair();
                match key.name() {
                    "bytes_counter" => assert_eq!(entry.unit, Some(Unit::Bytes)),
                    "time_gauge" => assert_eq!(entry.unit, Some(Unit::Milliseconds)),
                    "count_histogram" => assert_eq!(entry.unit, Some(Unit::Count)),
                    _ => panic!("Unexpected metric: {}", key.name()),
                }
            }
        });
    }

    #[test]
    #[cfg(feature = "otlp_integration")]
    fn otlp_allow_list() {
        assert!(should_export_to_otlp("fuse.op_latency_us"));
        assert!(should_export_to_otlp("s3.requests"));
        assert!(should_export_to_otlp("s3.meta_requests"));
        assert!(!should_export_to_otlp("some.random.metric"));
        assert!(!should_export_to_otlp("not.allowed"));
    }

    /// This is a manual test for verifying the integration of the metrics system with OpenTelemetry.
    /// It provides end-to-end verification of the metrics pipeline without needing to run the full mountpoint application.
    ///
    /// # Requirements
    /// - An OpenTelemetry collector running at the specified endpoint (default: http://localhost:4318/v1/metrics)
    ///
    /// # How to run
    /// ```bash
    /// # Start the OpenTelemetry collector (e.g., using Docker)
    /// docker run -p 4317:4317 -p 4318:4318 -v $(pwd)/collector-config.yaml:/etc/otel-collector-config.yaml \
    ///   otel/opentelemetry-collector:latest --config=/etc/otel-collector-config.yaml
    ///
    /// # Run the test with default endpoint (ignored by default)
    /// cargo test --package mountpoint-s3-fs --lib -- metrics::tests::otlp_metrics --exact --ignored
    ///
    /// # Or run with a custom endpoint by setting the MOUNTPOINT_TEST_OTLP_ENDPOINT environment variable
    /// MOUNTPOINT_TEST_OTLP_ENDPOINT="http://custom-server:4318/v1/metrics" cargo test --package mountpoint-s3-fs --lib -- metrics::tests::otlp_metrics --exact --ignored
    ///
    /// # Verify metrics in collector logs
    /// ```
    #[test]
    #[ignore]
    #[cfg(feature = "otlp_integration")]
    fn otlp_metrics() {
        use tracing::info;
        use tracing_subscriber::fmt::format::FmtSpan;
        use tracing_subscriber::util::SubscriberInitExt;

        // Initialize tracing for better test output
        tracing_subscriber::fmt()
            .with_span_events(FmtSpan::CLOSE)
            .with_target(false)
            .with_thread_ids(true)
            .with_level(true)
            .with_file(true)
            .with_line_number(true)
            .with_test_writer()
            .set_default();

        info!("Starting OTLP metrics test...");

        // Get OTLP endpoint from environment variable or use default
        let endpoint = std::env::var("MOUNTPOINT_TEST_OTLP_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:4318/v1/metrics".to_string());

        info!("Using OTLP endpoint: {}", endpoint);

        // Initialize metrics with an OTLP config
        let otlp_config = OtlpConfig::new(&endpoint).with_interval_secs(1);
        let sink = Arc::new(MetricsSink::new(Some(MetricsConfig::Otlp(otlp_config))).unwrap());
        let recorder = MetricsRecorder { sink: sink.clone() };

        with_local_recorder(&recorder, || {
            // Test counter with multiple labels
            let counter = metrics::counter!(
                "mountpoint_test_counter",
                "operation" => "write",
                "status" => "success",
                "test" => "true"
            );
            counter.increment(100);
            counter.increment(50);
            info!("Recorded counter with total value 150");

            // Test gauge with updates
            let gauge = metrics::gauge!(
                "mountpoint_test_gauge",
                "component" => "cache",
                "test" => "true"
            );
            gauge.set(1000.0);
            info!("Set gauge to 1000.0");
            gauge.set(500.0);
            info!("Updated gauge to 500.0");

            // Test histogram with multiple records
            let histogram = metrics::histogram!(
                "mountpoint_test_histogram",
                "operation" => "read",
                "test" => "true"
            );
            histogram.record(10.0);
            histogram.record(20.0);
            histogram.record(30.0);
            info!("Recorded histogram values: 10.0, 20.0, 30.0");

            // Publish metrics immediately to verify initial values
            info!("Publishing initial metrics...");
            sink.publish();

            // Sleep to allow metrics to be exported
            std::thread::sleep(std::time::Duration::from_secs(2));

            // Update metrics to verify changes are tracked
            counter.increment(200);
            gauge.set(750.0);
            histogram.record(40.0);
            info!("Updated all metrics with new values");

            // Publish again to verify updates
            info!("Publishing updated metrics...");
            sink.publish();

            // Wait for final export
            std::thread::sleep(std::time::Duration::from_secs(5));
            info!("Test complete. Metrics should show in collector logs.");
        });
    }

    #[test]
    #[cfg(feature = "otlp_integration")]
    fn test_otlp_endpoint_validation() {
        // Test with an invalid URI - we need to directly test the MetricsSink::new function
        // since install() will try to set up a global recorder which can only be done once
        let otlp_config = OtlpConfig::new("not-a-valid-uri");
        let result = MetricsSink::new(Some(MetricsConfig::Otlp(otlp_config)));
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("Invalid OTLP endpoint configuration"),
            "Error message should indicate invalid configuration: {error}",
        );

        // Test with no OTLP config (should succeed)
        let result = MetricsSink::new(None);
        assert!(result.is_ok());

        // Test with a syntactically valid endpoint (should succeed)
        let otlp_config = OtlpConfig::new("http://example.com:4318/v1/metrics");
        let result = MetricsSink::new(Some(MetricsConfig::Otlp(otlp_config)));
        assert!(result.is_ok());
    }

    #[test]
    #[cfg(feature = "otlp_integration")]
    fn test_otlp_real_time_recording() {
        use crate::metrics_otel::OtlpConfig;

        let otlp_config = OtlpConfig::new("http://localhost:4317");
        let sink = Arc::new(MetricsSink::new(Some(MetricsConfig::Otlp(otlp_config))).unwrap());
        let recorder = MetricsRecorder { sink: sink.clone() };

        with_local_recorder(&recorder, || {
            metrics::counter!("test_counter", "op" => "read").increment(42);
            metrics::gauge!("test_gauge", "status" => "active").set(100.0);
            metrics::histogram!("test_histogram", "method" => "get").record(1.5);
        });

        assert_eq!(sink.metrics.len(), 3);

        let metric_names: Vec<String> = sink
            .metrics
            .iter()
            .map(|entry| entry.key().name().to_string())
            .collect();

        assert!(metric_names.contains(&"test_counter".to_string()));
        assert!(metric_names.contains(&"test_gauge".to_string()));
        assert!(metric_names.contains(&"test_histogram".to_string()));
    }

    #[test]
    #[cfg(feature = "otlp_integration")]
    fn test_otlp_concurrent_updates() {
        use crate::metrics_otel::OtlpConfig;
        use std::thread;

        let otlp_config = OtlpConfig::new("http://localhost:4317");
        let sink = Arc::new(MetricsSink::new(Some(MetricsConfig::Otlp(otlp_config))).unwrap());
        let recorder = MetricsRecorder { sink: sink.clone() };

        // Set global recorder so threads can access it
        let _ = metrics::set_global_recorder(recorder);

        let handles: Vec<_> = (0..10)
            .map(|i| {
                thread::spawn(move || {
                    for j in 0..100 {
                        metrics::counter!("concurrent_counter").increment(1);
                        metrics::gauge!("concurrent_gauge").set(i as f64 + j as f64);
                        metrics::histogram!("concurrent_histogram").record((i + j) as f64);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(sink.metrics.len(), 3);
    }

    #[test]
    #[cfg(feature = "otlp_integration")]
    fn test_otlp_vs_no_otlp() {
        use crate::metrics_otel::OtlpConfig;

        let sink_no_otlp = Arc::new(MetricsSink::new(None).unwrap());
        let recorder_no_otlp = MetricsRecorder {
            sink: sink_no_otlp.clone(),
        };

        let otlp_config = OtlpConfig::new("http://localhost:4317");
        let sink_otlp = Arc::new(MetricsSink::new(Some(MetricsConfig::Otlp(otlp_config))).unwrap());
        let recorder_otlp = MetricsRecorder {
            sink: sink_otlp.clone(),
        };

        with_local_recorder(&recorder_no_otlp, || {
            metrics::counter!("test_counter").increment(1);
        });

        with_local_recorder(&recorder_otlp, || {
            metrics::counter!("test_counter").increment(1);
        });

        assert_eq!(sink_no_otlp.metrics.len(), 1);
        assert_eq!(sink_otlp.metrics.len(), 1);
    }
}
