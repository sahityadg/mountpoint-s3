# Benchmark experiment runner

This project allows to perform some Mountpoint benchmarks with different variables,
such that a number of experiments can be run with ease and the logs
and results be collected in a directory for each experiment run.

The Python script `benchmark.py` handles the setup and teardown for each experiment.
The experiment configuration space is managed using [Hydra](https://hydra.cc/).
Configurations in `conf/` describe which values to configure to run experiments over a set of parameters
such as the maximum count of Mountpoint FUSE workers,
number of application workers reading from unique file handles, etc..

The benchmark script supports multiple benchmark types, controlled by the `benchmark_type` parameter.

## Benchmark Types

The benchmark script supports the following benchmark types:

1. **FIO benchmark** (`benchmark_type=fio`) - Default benchmark type that runs FIO jobs defined in the configuration.

2. **Prefetch benchmark** (`benchmark_type=prefetch`) - Runs Mountpoint's prefetcher benchmarks to test the performance of prefetcher. 

## Before you start

You should have the environment setup where you want to run the benchmarking experiments.
For instance, this might be an EC2 instance. You also need an S3 bucket to run the workload against.

You should clone this repository to the environment. This tool will build Mountpoint for you.

This project uses [uv](https://github.com/astral-sh/uv) to manage Python environments and dependencies.

Think of `uv` as a close analog of Rust's _cargo_ but for Python.
It will automatically configure a Python virtual environment for you and install the project dependencies.

Assuming `uv` is installed, getting started is (almost) as easy as
running the `benchmark.py` script from this directory!

```sh
uv run benchmark.py --
```

It should tell you that you forgot some arguments for the Python script itself.

## Running the experiment

There are a few variables that are required, such as the S3 bucket used for testing.
You must set this in order to be able to use the benchmark script.

Additionally, you should configure the AWS credentials for Mountpoint.
You might use AWS profiles or set some credentials in the environment.

### Running the FIO benchmark (default)

To run the default FIO benchmark experiment:

```sh
uv run benchmark.py -- s3_bucket=amzn-s3-demo-bucket
```

Or explicitly specify the benchmark type:

```sh
uv run benchmark.py benchmark_type=fio -- s3_bucket=amzn-s3-demo-bucket
```

### Running the Prefetch benchmark

To run the prefetch benchmark:

```sh
uv run benchmark.py benchmark_type=prefetch -- s3_bucket=amzn-s3-demo-bucket
```

When using the prefetch benchmark, you can specify object keys to use:

```sh
uv run benchmark.py benchmark_type=prefetch -- s3_bucket=amzn-s3-demo-bucket \
    "objects=obj1,obj2,.."
```

Note: When passing object names from the command line, quote the entire parameter to avoid
iterating through each object key. When not specified, the tests fall back to using objects
created by fio: `j{i}_{object_size}GiB.bin`, with i starting from 0.

Output is written to `multirun/` within directories for the date, time, and experiment number run.
The output directory includes benchmark-specific files such as logs and results.

## Advanced configuration

### Configuring multiple network interfaces

When investigating performance with multiple network cards,
we need to tell Mountpoint about what network interfaces are available
and even configure it with things like a 'target throughput'
such that it allocates enough resources to maximize its utilisation of the available network bandwidth.

Below shows how to configure two network cards:

```sh
uv run benchmark.py -- s3_bucket=amzn-s3-demo-bucket \
    "network.interface_names=['eth0', 'eth1']" network.maximum_throughput_gbps=200
```

If you want to run experiments varying the interfaces provided to Mountpoint, you can vary it like below:

```sh
uv run benchmark.py -- s3_bucket=amzn-s3-demo-bucket \
    "network.interface_names=['eth0'], ['eth0', 'eth1']" network.maximum_throughput_gbps=200
```

If you want to do specific combinations, you will need to create full dictionaries to vary values over.
Below is an example of varying both network interfaces alongside the target network throughput.

```sh
uv run benchmark.py -- s3_bucket=amzn-s3-demo-bucket \
    "network={interface_names:['eth0'],maximum_throughput_gbps:100},{interface_names:['eth0','eth1'],maximum_throughput_gbps:200}"
```
