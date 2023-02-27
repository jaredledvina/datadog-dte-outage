#!/usr/bin/env python

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

import requests

LOG = logging.getLogger("datadog-dte-outage")
DATADOG_FLUSH_SECONDS = 10

def get_json(url):
    retries = 10
    for n in range(retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            break

        except requests.HTTPError as exc:
            code = exc.response.status_code
            LOG.error("Failed fetching %s : %s",url, code)            
            if code in [429, 500, 502, 503, 504]:
                # retry after n seconds
                time.sleep(n)
                continue
            raise
    return response.json()


def get_interval():
    interval_url = 'https://kubra.io/stormcenter/api/v1/stormcenters/4fbb3ad3-e01d-4d71-9575-d453769c1171/views/8ed2824a-bd92-474e-a7c4-848b812b7f9b/currentState?preview=false'
    json_data = get_json(interval_url)
    return json_data['data']['interval_generation_data']


def get_outage_data(interval_uuid):
    outage_data = []
    # thematic-1 is by county
    # thematic-2 is by zip code
    data_sources = {
        'county': 'https://kubra.io/{}/public/thematic-1/thematic_areas.json'.format(interval_uuid),
        'zip_code': 'https://kubra.io/{}/public/thematic-2/thematic_areas.json'.format(interval_uuid)
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
    dd_config = Configuration()
    body = MetricPayload(
        series=outage_data,
    )

    with ApiClient(dd_config) as api_client:
        api_instance = MetricsApi(api_client)
        LOG.info("Submitting %s metrics.", len(outage_data))
        response = api_instance.submit_metrics(body=body)
        if response['errors']:
            LOG.error("Error submitting metrics: %s", response['errors'])


def main():
    logging.basicConfig(level=logging.INFO)
    while True:
        LOG.info("Starting DTE Outage metric collection")
        interval_uuid = get_interval()
        outage_data = get_outage_data(interval_uuid)
        submit_outage_data(outage_data)
        LOG.info("Finished DTE Outage metric collection, waiting %s seconds", DATADOG_FLUSH_SECONDS)
        time.sleep(DATADOG_FLUSH_SECONDS)


if __name__ == '__main__':
  main()
