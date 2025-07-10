import json
import logging
import os
import re
import subprocess
import tempfile
from typing import Dict, Any

from benchmarks.base_benchmark import BaseBenchmark
from omegaconf import DictConfig

from benchmarks.benchmark_config_parser import BenchmarkConfigParser

log = logging.getLogger(__name__)


class CrtBenchmark(BaseBenchmark):
    def __init__(self, cfg: DictConfig):
        super().__init__(cfg)
        self.config_parser = BenchmarkConfigParser(cfg)
        self.common_config = self.config_parser.get_common_config()
        self.crt_config = self.config_parser.get_crt_config()

        crt_benchmarks_path = self.crt_config['crt_benchmarks_path']
        if crt_benchmarks_path is None:
            raise ValueError("CRT benchmarks path is not specified in the config file.")
        self.crt_benchmark_runner = f"{self.crt_benchmarks_path}/build/c/install/bin/s3-benchrunner-c"

    def _generate_benchmark_config(self, objects, object_size_in_gib, run_time) -> dict[str, Any]:
        config = {
            "version": 2,
            "filesOnDisk": False,
            "checksum": None,
            "maxRepeatCount": 1,
            "maxRepeatSecs": f"{run_time}",
            "tasks": [],
        }

        # Loop through objects and create tasks
        for object_key in objects:
            task = {
                "action": "download",
                "key": object_key,
                "size": object_size_in_gib * (1024 * 1024 * 1024),  # Convert GiB to bytes
            }
            config["tasks"].append(task)

        return config

    def setup(self) -> Dict[str, Any]:
        # Build CRT and create download json files
        object_size_in_gib = self.common_config['object_size_in_gib']
        app_workers = self.common_config['application_workers']
        run_time = self.common_config['run_time']
        objects = self.crt_config['objects']
        config = self._generate_benchmark_config(objects, object_size_in_gib, run_time)

        # create a tmp folder and write the file as /download-100GiB-1x-ram.run.json
        self.crt_cfg_file = tempfile.mktemp(suffix=f".download-{object_size_in_gib}-{app_workers}x-ram.run.json")

        # save the json output
        with open(self.crt_cfg_file, 'w') as f:
            json.dump(config, f, indent=4)

        # Build crt - scripts/build-runner.py --lang c --build-dir build
        subprocess_args = [
            f"{self.crt_benchmarks_path}/scripts/build-runner.py",
            "--lang",
            "c",
            "--build-dir",
            f"{self.crt_benchmarks_path}/build",
        ]

        if not os.path.exists(self.crt_benchmark_runner):
            try:
                subprocess.run(subprocess_args, check=True, capture_output=True, text=True)
                assert os.path.exists(self.crt_benchmark_runner)
                log.info("CRT build completed successfully.")
            except subprocess.CalledProcessError as e:
                raise RuntimeError("CRT build failed") from e

    def run_benchmark(self) -> Dict[str, Any]:
        # Run the command and capture the output
        # <crt_benchmark_runner> crt-c <json-config-file> <s3-bucket> <region> <max-throughput>

        subprocess_args = [
            self.crt_benchmark_runner,
            "crt-c",
            self.crt_cfg_file,
            self.crt_config['s3_bucket'],
            self.crt_config['region'],
            self.crt_config['max_throughput'],
        ]

        try:
            result = subprocess.run(subprocess_args, check=True, capture_output=True, text=True)
            log.info("CRT benchmark completed successfully.")
        except Exception as e:
            log.error(f"Error running CRT benchmark: {e}")
            raise RuntimeError("CRT benchmark failed") from e

        # Parse the output from run-1 as json and save the results in multirun folder
        # Run:1 Secs:56.572429 Gb/s:60.735838
        # Overall Throughput (Gb/s) Median:67.493451 Mean:66.580639 Min:60.735838 Max:68.889433 Variance:9.003807 StdDev:3.000635
        # Overall Duration (Secs) Median:50.908255 Mean:51.717910 Min:49.876646 Max:56.572429 Variance:6.147304 StdDev:2.479376
        # Peak RSS:8174.433594 MiB
        output = result.stdout

        self.parse_benchmark_output(output)

    def parse_benchmark_output(self, output):
        try:
            # Parse the output and extract the results
            # Parse single run result
            # Run:1 Secs:56.572429 Gb/s:60.735838
            run_pattern = r"Run:(\d+)\s+Secs:(\d+\.\d+)\s+Gb/s:(\d+\.\d+)"
            match = re.search(run_pattern, output)
            if match:
                duration_secs = float(match.group(2))
                throughput_gbps = float(match.group(3))
                json.dump(
                    {"duration_secs": duration_secs, "throughput_gbps": throughput_gbps},
                    open(f"{os.getcwd}/crt_output.json", 'w'),
                    indent=4,
                )
        except Exception as e:
            log.error(f"Error parsing CRT benchmark output: {e}")
            raise RuntimeError("CRT benchmark failed") from e

    def post_process(self) -> Dict[str, Any]:
        os.remove(self.crt_cfg_file)
        return {}
