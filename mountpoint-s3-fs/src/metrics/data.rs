use crate::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use crate::sync::{Arc, Mutex};

#[cfg(feature = "otlp_integration")]
use opentelemetry::KeyValue;

/// Represents the value of a metric
#[derive(Debug, Clone)]
pub enum MetricValue {
    Counter(u64),
    Gauge(f64),
    Histogram(f64),
}

/// A single metric
#[derive(Debug)]
pub enum Metric {
    Counter(Arc<ValueAndCount>),
    Gauge(Arc<AtomicGauge>),
    Histogram(Arc<Histogram>),
}

impl Metric {
    pub fn counter() -> Self {
        Self::Counter(Default::default())
    }

    pub fn as_counter(&self) -> metrics::Counter {
        let Metric::Counter(inner) = self else {
            panic!("not a counter");
        };
        metrics::Counter::from_arc(inner.clone())
    }

    pub fn gauge() -> Self {
        Self::Gauge(Default::default())
    }

    pub fn as_gauge(&self) -> metrics::Gauge {
        let Metric::Gauge(inner) = self else {
            panic!("not a gauge");
        };
        metrics::Gauge::from_arc(inner.clone())
    }

    pub fn histogram() -> Self {
        Self::Histogram(Arc::new(Histogram::new()))
    }

    pub fn as_histogram(&self) -> metrics::Histogram {
        let Metric::Histogram(inner) = self else {
            panic!("not a histogram");
        };
        metrics::Histogram::from_arc(inner.clone())
    }

    /// Generate the string representation of this metric.
    /// Returns None if the metric has had no values emitted since the last call to this function.
    pub fn fmt_and_reset(&self) -> Option<String> {
        match self {
            Metric::Counter(inner) => {
                let (sum, n) = inner.load_and_reset()?;
                let fmt = if n == 1 {
                    format!("{sum}")
                } else {
                    format!("{sum} (n={n})")
                };
                Some(fmt)
            }
            // Gauges can't reset because they can be incremented/decremented
            Metric::Gauge(inner) => {
                let value = inner.load_if_changed()?;
                Some(format!("{value}"))
            }
            Metric::Histogram(histogram) => histogram.run_and_reset(|histogram| {
                format!(
                    "n={}: min={} p10={} p50={} avg={:.2} p90={} p99={} p99.9={} max={}",
                    histogram.len(),
                    histogram.min(),
                    histogram.value_at_quantile(0.1),
                    histogram.value_at_quantile(0.5),
                    histogram.mean(),
                    histogram.value_at_quantile(0.9),
                    histogram.value_at_quantile(0.99),
                    histogram.value_at_quantile(0.999),
                    histogram.max(),
                )
            }),
        }
    }
}

#[derive(Debug, Default)]
pub struct ValueAndCount {
    pub sum: AtomicU64,
    pub n: AtomicUsize,
    #[cfg(feature = "otlp_integration")]
    otlp_counter: Option<opentelemetry::metrics::Counter<u64>>,
    #[cfg(feature = "otlp_integration")]
    attributes: Vec<KeyValue>,
}

impl metrics::CounterFn for ValueAndCount {
    fn increment(&self, value: u64) {
        self.sum.fetch_add(value, Ordering::SeqCst);
        self.n.fetch_add(1, Ordering::SeqCst);

        #[cfg(feature = "otlp_integration")]
        if let Some(otlp_counter) = &self.otlp_counter {
            otlp_counter.add(value, &self.attributes);
        }
    }

    fn absolute(&self, value: u64) {
        self.sum.store(value, Ordering::SeqCst);
        self.n.store(1, Ordering::SeqCst);

        #[cfg(feature = "otlp_integration")]
        if let Some(otlp_counter) = &self.otlp_counter {
            otlp_counter.add(value, &self.attributes);
        }
    }
}

impl ValueAndCount {
    #[cfg(feature = "otlp_integration")]
    pub fn with_otlp(otlp_counter: opentelemetry::metrics::Counter<u64>, attributes: Vec<KeyValue>) -> Self {
        Self {
            sum: AtomicU64::new(0),
            n: AtomicUsize::new(0),
            otlp_counter: Some(otlp_counter),
            attributes,
        }
    }

    pub fn load_and_reset(&self) -> Option<(u64, usize)> {
        let sum = self.sum.swap(0, Ordering::SeqCst);
        let n = self.n.swap(0, Ordering::SeqCst);
        if n == 0 { None } else { Some((sum, n)) }
    }
}

/// An atomic gauge.
///
/// Gauges are floats but there's no atomic floats in std, so we stuff the float into an AtomicU64
/// by converting to/from the bit representation.
#[derive(Debug, Default)]
pub struct AtomicGauge {
    bits: AtomicU64,
    changed: AtomicBool,
    #[cfg(feature = "otlp_integration")]
    otlp_gauge: Option<opentelemetry::metrics::Gauge<f64>>,
    #[cfg(feature = "otlp_integration")]
    attributes: Vec<KeyValue>,
}

impl metrics::GaugeFn for AtomicGauge {
    fn increment(&self, value: f64) {
        self.update(|old| old + value);
    }

    fn decrement(&self, value: f64) {
        self.update(|old| old - value);
    }

    fn set(&self, value: f64) {
        self.update(|_old| value);
    }
}

impl AtomicGauge {
    #[cfg(feature = "otlp_integration")]
    pub fn with_otlp(otlp_gauge: opentelemetry::metrics::Gauge<f64>, attributes: Vec<KeyValue>) -> Self {
        Self {
            bits: AtomicU64::new(0.0_f64.to_bits()),
            changed: AtomicBool::new(false),
            otlp_gauge: Some(otlp_gauge),
            attributes,
        }
    }

    fn update(&self, f: impl Fn(f64) -> f64) {
        let new_bits = self
            .bits
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, move |old_bits| {
                Some(f(f64::from_bits(old_bits)).to_bits())
            })
            .expect("closure always returns Some");
        self.changed.store(true, Ordering::SeqCst);

        #[cfg(feature = "otlp_integration")]
        if let Some(otlp_gauge) = &self.otlp_gauge {
            otlp_gauge.record(f64::from_bits(new_bits), &self.attributes);
        }
    }

    /// Return the current value of this gauge if it has changed since the last call to this method.
    /// Note that "changed" just means another `gauge!()` call has occurred; the actual value may
    /// still be the same.
    pub fn load_if_changed(&self) -> Option<f64> {
        if self.changed.swap(false, Ordering::SeqCst) {
            Some(f64::from_bits(self.bits.load(Ordering::SeqCst)))
        } else {
            None
        }
    }
}

/// An auto-resizing histogram with a precision of two significant figures.
#[derive(Debug)]
pub struct Histogram {
    histogram: Mutex<hdrhistogram::Histogram<u64>>,
    #[cfg(feature = "otlp_integration")]
    otlp_histogram: Option<opentelemetry::metrics::Histogram<f64>>,
    #[cfg(feature = "otlp_integration")]
    attributes: Vec<KeyValue>,
}

impl metrics::HistogramFn for Histogram {
    fn record(&self, value: f64) {
        self.histogram
            .lock()
            .unwrap()
            .record(value as u64)
            .expect("histogram should always resize when value is too large");

        #[cfg(feature = "otlp_integration")]
        if let Some(otlp_histogram) = &self.otlp_histogram {
            otlp_histogram.record(value, &self.attributes);
        }
    }
}

impl Histogram {
    fn new() -> Self {
        let histogram = hdrhistogram::Histogram::new(2).unwrap();
        Self {
            histogram: Mutex::new(histogram),
            #[cfg(feature = "otlp_integration")]
            otlp_histogram: None,
            #[cfg(feature = "otlp_integration")]
            attributes: Vec::new(),
        }
    }

    #[cfg(feature = "otlp_integration")]
    pub fn with_otlp(otlp_histogram: opentelemetry::metrics::Histogram<f64>, attributes: Vec<KeyValue>) -> Self {
        let histogram = hdrhistogram::Histogram::new(2).unwrap();
        Self {
            histogram: Mutex::new(histogram),
            otlp_histogram: Some(otlp_histogram),
            attributes,
        }
    }

    /// If this histogram has any data, run the closure, reset the histogram, and return the closure
    /// result. Otherwise return None.
    pub fn run_and_reset<T>(&self, f: impl FnOnce(&hdrhistogram::Histogram<u64>) -> T) -> Option<T> {
        let mut histogram = self.histogram.lock().unwrap();
        if histogram.is_empty() {
            return None;
        }

        let result = f(&histogram);
        histogram.reset();
        Some(result)
    }
}
