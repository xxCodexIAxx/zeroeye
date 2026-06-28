#!/usr/bin/env python3
import importlib.util
import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "tools" / "health_check.py"

spec = importlib.util.spec_from_file_location("health_check", MODULE_PATH)
health_check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(health_check)


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class HealthCheckRateLimitTests(unittest.TestCase):
    def test_parse_global_timeout_override(self):
        default_timeout, overrides = health_check.parse_timeout_overrides("2.5")

        self.assertEqual(default_timeout, 2.5)
        self.assertEqual(overrides, {})

    def test_parse_per_service_timeout_overrides(self):
        default_timeout, overrides = health_check.parse_timeout_overrides("backend=1.5, market=3")

        self.assertIsNone(default_timeout)
        self.assertEqual(overrides["backend"], 1.5)
        self.assertEqual(overrides["market"], 3.0)

    def test_token_bucket_throttles_above_configured_rate(self):
        clock = FakeClock()
        limiter = health_check.TokenBucketRateLimiter(2, clock.time, clock.sleep)

        limiter.wait_for_token()
        limiter.wait_for_token()
        limiter.wait_for_token()

        self.assertEqual(limiter.throttled_requests, 1)
        self.assertAlmostEqual(clock.sleeps[0], 0.5)

    def test_half_open_circuit_reduces_effective_rate(self):
        clock = FakeClock()
        limiter = health_check.TokenBucketRateLimiter(4, clock.time, clock.sleep)

        limiter.wait_for_token(half_open=True)

        self.assertEqual(limiter.stats()["current_rate"], 2.0)

    def test_run_health_checks_applies_timeout_and_reports_rate_stats(self):
        calls = []
        original_http = health_check.check_http_service
        original_disk = health_check.check_disk_usage
        original_memory = health_check.check_memory_usage
        original_load = health_check.check_load_average

        def fake_http(host, port, path, timeout):
            calls.append((host, port, path, timeout))
            return "OK", "HTTP 200", 200

        try:
            health_check.check_http_service = fake_http
            health_check.check_disk_usage = lambda: ("OK", "disk ok", 10)
            health_check.check_memory_usage = lambda: ("OK", "memory ok", 10)
            health_check.check_load_average = lambda: ("OK", "load ok", 0.1)

            clock = FakeClock()
            limiter = health_check.TokenBucketRateLimiter(10, clock.time, clock.sleep)
            results = health_check.run_health_checks(
                service="backend",
                timeout_arg="backend=1.25",
                probe_rate=10,
                circuit_states={"backend": "HALF_OPEN"},
                rate_limiter=limiter,
            )
        finally:
            health_check.check_http_service = original_http
            health_check.check_disk_usage = original_disk
            health_check.check_memory_usage = original_memory
            health_check.check_load_average = original_load

        self.assertEqual(calls[0][3], 1.25)
        self.assertEqual(results["services"]["backend"]["timeout"], 1.25)
        self.assertEqual(results["services"]["backend"]["circuit_state"], "HALF_OPEN")
        self.assertEqual(results["rate_limiter"]["current_rate"], 5.0)
        self.assertEqual(results["overall_status"], "OK")


if __name__ == "__main__":
    unittest.main()
