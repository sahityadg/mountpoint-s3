import logging
import subprocess
from typing import Dict, Any

from benchmarks.base_benchmark import BaseBenchmark
from omegaconf import DictConfig

from benchmarks.benchmark_config_parser import BenchmarkConfigParser

log = logging.getLogger(__name__)


class PrefetchBenchmark(BaseBenchmark):
    def __init__(self, cfg: DictConfig):
        super().__init__(cfg)
        self.config_parser = BenchmarkConfigParser(cfg)
        self.common_config = self.config_parser.get_common_config()
        self.prefetch_config = self.config_parser.get_prefetch_config()

    def setup(self) -> Dict[str, Any]:
        # Prefetch benchmark doesn't need to mount an S3 bucket
        # It uses the cargo run example directly
        return {}

    def run_benchmark(self) -> Dict[str, Any]:
        """
        Run the prefetch benchmark against the file system.
        This benchmark tests the prefetching capabilities of Mountpoint-S3.
        """
        try:
            subprocess_args = [
                "cargo",
                "run",
                "--example",
                "prefetch_benchmark",
                self.common_config['s3_bucket'],
            ]

            # Generate S3 keys based on application_workers
            object_size = self.common_config['object_size_in_gib']
            size_gib = str(object_size)
            app_workers = self.common_config['application_workers']

            # When objects are not specified, it tries to reuse objects
            # from fio tests
            objects = self.prefetch_config['objects']
            if not objects:
                for i in range(app_workers):
                    subprocess_args.append(f"j{i}_{size_gib}GiB.bin")
            else:
                if len(objects) >= app_workers:
                    for i in range(app_workers):
                        subprocess_args.append(objects[i])
                else:
                    raise ValueError("Seeing fewer objects than app workers. So cannot proceed with the run.")

            # Add optional parameters
            region = self.cfg.get("region", "us-east-1")
            subprocess_args.extend(["--region", region])

            max_throughput = self.common_config['max_throughput_gbps']
            if max_throughput is not None:
                subprocess_args.extend(["--maximum-throughput-gbps", str(max_throughput)])

            max_memory_target = self.prefetch_config['max_memory_target']
            if max_memory_target is not None:
                subprocess_args.extend(["--max-memory-target", str(max_memory_target)])

            read_part_size = self.common_config['read_part_size']
            if read_part_size is not None:
                subprocess_args.extend(["--part-size", str(read_part_size)])

            read_size = self.common_config['read_size']
            subprocess_args.extend(["--read-size", str(read_size)])

            for interface in self.common_config['network_interfaces']:
                subprocess_args.extend(["--bind", interface])

            run_time = self.common_config['run_time']
            if run_time is not None:
                subprocess_args.extend(["--runtime", str(run_time)])

            # Create log file for both stdout and stderr
            log_file = "prefetch_benchmark.log"

            log.info("Running prefetch benchmark with args: %s", subprocess_args)

            # Execute the command with redirection to log file
            with open(log_file, 'w') as f:
                process = subprocess.Popen(
                    subprocess_args,
                    stdout=f,
                    stderr=subprocess.STDOUT,  # Redirect stderr to stdout, which goes to the log file
                )
                exit_code = process.wait()

            if exit_code != 0:
                log.error(f"Prefetch benchmark failed with exit code {exit_code}")
                return {"success": False}
            else:
                log.info("Prefetch benchmarks completed successfully")

            return {"success": True}
        except Exception as e:
            log.error(f"Benchmark failed: {e}")
            return {"success": False}

    def post_process(self) -> Dict[str, Any]:
        # No specific post-processing needed for prefetch benchmark
        return {}
