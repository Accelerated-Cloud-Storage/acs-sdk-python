#!/usr/bin/env python3

import os
import time
import random
import string
import statistics
from typing import List, Dict, Tuple
from dataclasses import dataclass
from pathlib import Path

@dataclass
class TestResult:
    operation: str
    file_size: int
    batch_size: int
    total_time: float
    latencies: List[float]
    throughput: float

def generate_random_data(size: int) -> bytes:
    """Generate random data of specified size."""
    return os.urandom(size)

def generate_random_filename() -> str:
    """Generate a random filename with .txt extension."""
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for _ in range(10)) + '.txt'

class FUSEBenchmark:
    def __init__(self, mount_path: str):
        self.mount_path = Path(mount_path)
        self.sizes = [1024, 1024*1024, 10*1024*1024]  # 1KB, 1MB, 10MB
        self.batch_size = 50
        self.results: List[TestResult] = []

    def run_write_test(self, size: int, files: List[Path]) -> TestResult:
        """Test write operations for given file size."""
        print(f"\nTesting writes with {size/1024:.0f}KB files...")
        latencies = []
        start_time = time.time()

        for filename in files:
            data = generate_random_data(size)
            write_start = time.time()
            with open(filename, 'wb') as f:
                f.write(data)
            latencies.append(time.time() - write_start)

        total_time = time.time() - start_time
        throughput = (size * self.batch_size) / total_time  # bytes per second

        return TestResult(
            operation='write',
            file_size=size,
            batch_size=self.batch_size,
            total_time=total_time,
            latencies=latencies,
            throughput=throughput
        )

    def run_read_test(self, size: int, files: List[Path]) -> TestResult:
        """Test read operations for given file size."""
        print(f"Testing reads with {size/1024:.0f}KB files...")
        latencies = []
        start_time = time.time()

        for filename in files:
            read_start = time.time()
            with open(filename, 'rb') as f:
                data = f.read()
            latencies.append(time.time() - read_start)
            assert len(data) == size

        total_time = time.time() - start_time
        throughput = (size * self.batch_size) / total_time

        return TestResult(
            operation='read',
            file_size=size,
            batch_size=self.batch_size,
            total_time=total_time,
            latencies=latencies,
            throughput=throughput
        )

    def run_delete_test(self, files: List[Path]) -> TestResult:
        """Test delete operations."""
        print("Testing deletes...")
        latencies = []
        start_time = time.time()

        for filename in files:
            delete_start = time.time()
            os.remove(filename)
            latencies.append(time.time() - delete_start)

        total_time = time.time() - start_time
        throughput = self.batch_size / total_time  # operations per second

        return TestResult(
            operation='delete',
            file_size=0,  # Not relevant for deletes
            batch_size=self.batch_size,
            total_time=total_time,
            latencies=latencies,
            throughput=throughput
        )

    def run_benchmarks(self):
        """Run all benchmarks."""
        for size in self.sizes:
            # Create test files (generate filenames once)
            files = [self.mount_path / generate_random_filename() 
                     for _ in range(self.batch_size)]
            
            # Run tests using the same file list for writes, reads, deletes
            write_result = self.run_write_test(size, files)
            read_result = self.run_read_test(size, files)
            delete_result = self.run_delete_test(files)

            # Store results
            self.results.extend([write_result, read_result, delete_result])

    def print_results(self):
        """Print benchmark results."""
        print("\nBenchmark Results:")
        print("=" * 80)

        for size in self.sizes:
            print(f"\nResults for {size/1024:.0f}KB files:")
            print("-" * 40)
            
            size_results = [r for r in self.results if r.file_size == size or r.operation == 'delete']
            for result in size_results:
                avg_latency = statistics.mean(result.latencies) * 1000  # Convert to ms
                p95_latency = statistics.quantiles(result.latencies, n=20)[-1] * 1000
                
                throughput_unit = "ops/sec" if result.operation == 'delete' else "MB/sec"
                throughput = result.throughput if result.operation == 'delete' else result.throughput / (1024*1024)
                
                print(f"\n{result.operation.capitalize()}:")
                print(f"  Average Latency: {avg_latency:.2f}ms")
                print(f"  P95 Latency: {p95_latency:.2f}ms")
                print(f"  Throughput: {throughput:.2f} {throughput_unit}")

def main():
    """Run the benchmark suite."""
    import argparse
    parser = argparse.ArgumentParser(description='FUSE Filesystem Benchmark')
    parser.add_argument('mount_path', help='Path to the mounted filesystem')
    args = parser.parse_args()

    if not os.path.ismount(args.mount_path):
        print(f"Error: {args.mount_path} is not a mount point")
        return 1

    benchmark = FUSEBenchmark(args.mount_path)
    try:
        benchmark.run_benchmarks()
        benchmark.print_results()
    except Exception as e:
        print(f"Benchmark failed: {e}")
        return 1

if __name__ == '__main__':
    exit(main())
