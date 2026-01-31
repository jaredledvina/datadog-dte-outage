#!/usr/bin/env python
"""Scrapes https://outagemap.dteenergy.com and sends metrics to Datadog."""

import json
import logging
import os
import random
import signal
import time
from datetime import datetime

from curl_cffi import CurlError
from curl_cffi import requests as curl_requests
from curl_cffi.requests import RequestsError
from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v1.api.service_checks_api import ServiceChecksApi
from datadog_api_client.v1.model.service_check import ServiceCheck
from datadog_api_client.v1.model.service_check_status import ServiceCheckStatus
from datadog_api_client.v1.model.service_checks import ServiceChecks
from datadog_api_client.v2.api.metrics_api import MetricsApi
from datadog_api_client.v2.model.metric_intake_type import MetricIntakeType
from datadog_api_client.v2.model.metric_payload import MetricPayload
from datadog_api_client.v2.model.metric_point import MetricPoint
from datadog_api_client.v2.model.metric_resource import MetricResource
from datadog_api_client.v2.model.metric_series import MetricSeries

# Configuration
VERSION = os.environ.get("APP_VERSION", "dev")
POLL_INTERVAL = 10
MAX_RETRIES = 5
REQUEST_TIMEOUT = 10
MAX_CONSECUTIVE_FAILURES = 10
CIRCUIT_BREAKER_COOLDOWN = 300
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

# API endpoints
KUBRA_STATE_URL = (
    "https://kubra.io/stormcenter/api/v1/stormcenters/"
    "4fbb3ad3-e01d-4d71-9575-d453769c1171/views/"
    "8ed2824a-bd92-474e-a7c4-848b812b7f9b/currentState?preview=false"
)
DTE_SITUATIONS_URL = "https://outage.dteenergy.com/situations.json"

# Shared resource for all metrics
METRIC_RESOURCE = MetricResource(name="dte-outage", type="host")

# Browser headers to avoid bot detection
REQUEST_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://outage.dteenergy.com/",
    "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

LOG = logging.getLogger("datadog-dte-outage")

# Module state
_session = None
_etag_cache = {}
_consecutive_failures = 0
_shutdown_requested = False


def _get_session():
    """Get or create the HTTP session with browser impersonation."""
    global _session
    if _session is None:
        _session = curl_requests.Session()
        _session.headers.update(REQUEST_HEADERS)
    return _session


def _handle_shutdown(signum, frame):
    """Handle SIGTERM/SIGINT for graceful shutdown."""
    global _shutdown_requested
    LOG.info("Shutdown signal received, finishing current cycle...")
    _shutdown_requested = True


def create_metric(name, value, timestamp, tags):
    """Create a Datadog MetricSeries with common defaults."""
    return MetricSeries(
        metric=name,
        type=MetricIntakeType.GAUGE,
        points=[MetricPoint(timestamp=timestamp, value=float(value))],
        tags=tags,
        resources=[METRIC_RESOURCE],
    )


def fetch_json(url):
    """
    Fetch JSON from a URL with retries, caching, and bot detection avoidance.

    Uses Chrome TLS fingerprint impersonation and conditional requests via ETags.
    """
    global _consecutive_failures

    cached = _etag_cache.get(url, {})
    session = _get_session()

    # Small random delay to appear more human
    time.sleep(random.uniform(0.5, 1.5))

    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            headers = {"If-None-Match": cached["etag"]} if "etag" in cached else {}
            response = session.get(
                url,
                timeout=REQUEST_TIMEOUT,
                headers=headers,
                impersonate="chrome131",
            )

            # Return cached data if not modified
            if response.status_code == 304 and "data" in cached:
                LOG.debug("Using cached data for %s", url)
                _consecutive_failures = 0
                return cached["data"]

            # Handle retryable HTTP errors before raise_for_status
            if response.status_code in RETRYABLE_STATUS_CODES:
                LOG.error("HTTP %s fetching %s", response.status_code, url)
                retry_after = response.headers.get("Retry-After")
                sleep_time = int(retry_after) if retry_after else min(2 ** attempt, 60)
                LOG.info("Retrying in %ss...", sleep_time)
                last_error = RequestsError(f"HTTP {response.status_code}", response=response)
                time.sleep(sleep_time)
                continue

            # Raise for other non-success status codes (4xx errors won't be retried)
            response.raise_for_status()

            # Detect bot protection (HTML instead of JSON)
            content_type = response.headers.get("content-type", "")
            if "html" in content_type.lower():
                raise ValueError(f"Received HTML instead of JSON: {response.text[:200]}")

            data = response.json()

            # Cache response with ETag if available
            if "etag" in response.headers:
                _etag_cache[url] = {"etag": response.headers["etag"], "data": data}

            _consecutive_failures = 0
            return data

        except (CurlError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
            LOG.error("Error fetching %s: %s", url, exc)
            last_error = exc
            time.sleep(min(2 ** attempt, 60))

    _consecutive_failures += 1
    raise last_error or RuntimeError(f"Failed to fetch {url} after {MAX_RETRIES} retries")


def collect_outage_metrics():
    """Collect all outage metrics from DTE APIs."""
    metrics = []
    timestamp = int(datetime.now().timestamp())

    # Get current interval UUID for Kubra API
    state = fetch_json(KUBRA_STATE_URL)
    interval_uuid = state["data"]["interval_generation_data"]

    # Collect county and zip code data
    sources = {
        "county": f"https://kubra.io/{interval_uuid}/public/thematic-1/thematic_areas.json",
        "zip_code": f"https://kubra.io/{interval_uuid}/public/thematic-2/thematic_areas.json",
    }

    for source_name, url in sources.items():
        LOG.info("Fetching %s data", source_name)
        data = fetch_json(url)

        for area in data["file_data"]:
            name = area["desc"]["name"]
            tag = f"{source_name}:{name}"

            metrics.append(create_metric(
                f"dte.outage.{source_name}.current",
                area["desc"]["cust_a"]["val"],
                timestamp,
                [tag],
            ))
            metrics.append(create_metric(
                f"dte.outage.{source_name}.total",
                area["desc"]["cust_s"],
                timestamp,
                [tag],
            ))

    # Collect situation summary data
    LOG.info("Fetching situations data")
    situations = fetch_json(DTE_SITUATIONS_URL)

    for key, value in situations.items():
        if key not in {"lastUpdated", "currentSituations"}:
            metrics.append(create_metric(
                f"dte.outage.situations.{key}",
                value,
                timestamp,
                [f"{key}:{value}"],
            ))

    for situation in situations["currentSituations"]:
        metrics.append(create_metric(
            f"dte.outage.situations.{situation['key']}",
            situation["displayValue"],
            timestamp,
            [f"currentSituations:{situation['key']}"],
        ))

    return metrics


def submit_metrics(metrics):
    """Submit metrics to Datadog."""
    config = Configuration()
    with ApiClient(config) as client:
        api = MetricsApi(client)
        LOG.info("Submitting %s metrics", len(metrics))
        response = api.submit_metrics(body=MetricPayload(series=metrics))
        if response.get("errors"):
            LOG.error("Error submitting metrics: %s", response["errors"])


def submit_health_check():
    """Submit health check to Datadog."""
    config = Configuration()
    body = ServiceChecks([
        ServiceCheck(
            check="dte.outage.ok",
            host_name="dte-outage",
            status=ServiceCheckStatus.OK,
            tags=[],
        )
    ])
    with ApiClient(config) as client:
        api = ServiceChecksApi(client)
        LOG.info("Submitting health check")
        response = api.submit_service_check(body=body)
        if response.get("status") != "ok":
            LOG.error("Error submitting health check: %s", response.get("status"))


def main():
    """Main collection loop."""
    global _consecutive_failures

    logging.basicConfig(level=logging.INFO)
    LOG.info("datadog-dte-outage %s starting", VERSION)

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    while not _shutdown_requested:
        # Circuit breaker
        if _consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            LOG.warning(
                "Circuit breaker: %s failures, cooling down %ss",
                _consecutive_failures,
                CIRCUIT_BREAKER_COOLDOWN,
            )
            time.sleep(CIRCUIT_BREAKER_COOLDOWN)
            _consecutive_failures = 0

        try:
            LOG.info("Starting collection cycle")
            metrics = collect_outage_metrics()

            if metrics:
                submit_metrics(metrics)
                submit_health_check()
            else:
                LOG.warning("No metrics collected")

        except Exception as exc:
            LOG.error("Collection failed: %s", exc, exc_info=True)
            _consecutive_failures += 1

        # Sleep with jitter
        sleep_time = POLL_INTERVAL + random.uniform(-2, 2)
        LOG.info("Next collection in %.1fs", sleep_time)
        time.sleep(sleep_time)

    LOG.info("Shutdown complete")


if __name__ == "__main__":
    main()
