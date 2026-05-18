import time
import pytest
from _dpdk_sim import ffi, lib

def test_dpdk_init_and_cleanup():
    lib.dpdk_init()
    lib.dpdk_cleanup()

def test_dpdk_read_empty():
    lib.dpdk_init()
    out_data = ffi.new("uint8_t[]", 64)
    res = lib.dpdk_read(out_data)
    assert res == 0
    lib.dpdk_cleanup()

def test_dpdk_simulate_and_read():
    lib.dpdk_init()

    test_msg = b"hello dpdk"
    msg_len = len(test_msg)
    data = ffi.new("uint8_t[]", test_msg)

    lib.dpdk_simulate_packet(data, msg_len)

    out_data = ffi.new("uint8_t[]", 64)
    res = lib.dpdk_read(out_data)

    assert res == 1
    out_bytes = bytes(out_data)[:msg_len]
    assert out_bytes == test_msg

    lib.dpdk_cleanup()

def test_dpdk_latency():
    lib.dpdk_init()

    test_msg = b"latency test"
    msg_len = len(test_msg)
    data = ffi.new("uint8_t[]", test_msg)

    out_data = ffi.new("uint8_t[]", 64)

    # Warmup
    for _ in range(100):
        lib.dpdk_simulate_packet(data, msg_len)
        lib.dpdk_read(out_data)

    latencies = []

    for _ in range(1000):
        lib.dpdk_simulate_packet(data, msg_len)
        start_time = time.perf_counter_ns()
        res = lib.dpdk_read(out_data)
        end_time = time.perf_counter_ns()

        assert res == 1
        latencies.append(end_time - start_time)

    avg_latency_ns = sum(latencies) / len(latencies)
    avg_latency_us = avg_latency_ns / 1000.0

    print(f"Average read latency: {avg_latency_us:.3f} microseconds")

    # The requirement is sub-10 microsecond latency
    assert avg_latency_us < 10.0

    with open("logs/dpdk_benchmark.txt", "w") as f:
        f.write(f"DPDK Benchmark Results:\n")
        f.write(f"Average Latency: {avg_latency_us:.3f} microseconds\n")
        f.write(f"Sub-10us Requirement Met: {'Yes' if avg_latency_us < 10.0 else 'No'}\n")

    lib.dpdk_cleanup()
