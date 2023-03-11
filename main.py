#!/usr/bin/env python

"""
datadog-dte-outage - Scrapes https://outagemap.dteenergy.com & sends Datadog metrics
"""

from datetime import datetime
import time
import logging

from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v2.api.metrics_api import MetricsApi
from datadog_api_client.v2.model.metric_intake_type import MetricIntakeType
from datadog_api_client.v2.model.metric_payload import MetricPayload
from datadog_api_client.v2.model.metric_point import MetricPoint
from datadog_api_client.v2.model.metric_resource import MetricResource
from datadog_api_client.v2.model.metric_series import MetricSeries
from datadog_api_client.v1.api.service_checks_api import ServiceChecksApi
from datadog_api_client.v1.model.service_check import ServiceCheck
from datadog_api_client.v1.model.service_check_status import ServiceCheckStatus
from datadog_api_client.v1.model.service_checks import ServiceChecks

import requests

LOG = logging.getLogger("datadog-dte-outage")
DATADOG_FLUSH_SECONDS = 10

def get_json(url):
    """
    get_json takes a URl and returns the JSON data
    """
    retries = 10
    for retry in range(retries):
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            break
        except requests.exceptions.HTTPError as exc:
            code = exc.response.status_code
            LOG.error("Failed fetching %s : %s", url, code)
            if code in [429, 500, 502, 503, 504]:
                # retry after n seconds
                time.sleep(retry)
                continue
            raise
        except requests.exceptions.Timeout as exc:
            LOG.error("Timed out fetching %s ", url)
            LOG.error(exc, exc_info=True)
            time.sleep(retry)
            continue
    return response.json()


def get_interval():
    """
    get_interval returns the current data generation interval UUID 
    """
    # pylint: disable=line-too-long
    interval_url = 'https://kubra.io/stormcenter/api/v1/stormcenters/4fbb3ad3-e01d-4d71-9575-d453769c1171/views/8ed2824a-bd92-474e-a7c4-848b812b7f9b/currentState?preview=false'
    json_data = get_json(interval_url)
    return json_data['data']['interval_generation_data']


def get_outage_data(interval_uuid):
    """
    get_outage_data returns a list of Datadog MetricSeries for all outage data
    """
    outage_data = []
    # thematic-1 is by county
    # thematic-2 is by zip code
    data_sources = {
        'county': f"https://kubra.io/{interval_uuid}/public/thematic-1/thematic_areas.json",
        'zip_code': f"https://kubra.io/{interval_uuid}/public/thematic-2/thematic_areas.json"
    }

    for source, target in data_sources.items():
        LOG.info("Fetching %s data", source)
        json_data = get_json(target)
        fetch_timestamp = int(datetime.now().timestamp())

        for data in json_data['file_data']:
            resource = data['desc']['name']
            outage_data.append(MetricSeries(
                metric='dte.outage.' + source + '.' + 'current',
                type=MetricIntakeType.GAUGE,
                points=[
                    MetricPoint(
                        timestamp=fetch_timestamp,
                        value=data['desc']['cust_a']['val'],
                    ),
                ],
                tags=[ source + ':' + resource ],
                resources=[
                    MetricResource(
                        name="dte-outage",
                        type="host",
                    ),
                ],
            ))
            outage_data.append(MetricSeries(
                metric='dte.outage.' + source + '.' + 'total',
                type=MetricIntakeType.GAUGE,
                points=[
                    MetricPoint(
                        timestamp=fetch_timestamp,
                        value=data['desc']['cust_s'],
                    ),
                ],
                tags=[ source + ':' + resource ],
                resources=[
                    MetricResource(
                        name="dte-outage",
                        type="host",
                    ),
                ],
            ))
    return outage_data


def submit_outage_data(outage_data):
    """
    submit_outage_data sends a list of MetricSeries to Datadag
    """
    dd_config = Configuration()
    body = MetricPayload(
        series=outage_data,
    )

    with ApiClient(dd_config) as api_client:
        api_instance = MetricsApi(api_client)
        LOG.info("Submitting %s metrics", len(outage_data))
        response = api_instance.submit_metrics(body=body)
        if response['errors']:
            LOG.error("Error submitting metrics: %s", response['errors'])


def submit_health_check():
    """
    submit_health_check sends a Service Check for monitoring 
    """
    body = ServiceChecks(
        [
            ServiceCheck(
                check="dte.outage.ok",
                host_name="dte-outage",
                status=ServiceCheckStatus.OK,
                tags=[],
            ),
        ]
    )

    configuration = Configuration()
    with ApiClient(configuration) as api_client:
        api_instance = ServiceChecksApi(api_client)
        LOG.info("Submitting health check")
        response = api_instance.submit_service_check(body=body)
        if response['status'] != 'ok':
            LOG.error("Error submitting service check: %s", response['status'])


def main():
    """
    main does the thing
    """
    logging.basicConfig(level=logging.INFO)
    while True:
        LOG.info("Starting DTE Outage metric collection")
        interval_uuid = get_interval()
        outage_data = get_outage_data(interval_uuid)
        submit_outage_data(outage_data)
        submit_health_check()
        LOG.info("Finished DTE Outage metric collection, waiting %s seconds", DATADOG_FLUSH_SECONDS)
        time.sleep(DATADOG_FLUSH_SECONDS)


if __name__ == '__main__':
    main()
