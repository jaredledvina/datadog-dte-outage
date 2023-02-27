# datadog-dte-outage 

This is a small Python script that scrapes the data availabe from https://outagemap.dteenergy.com
and emits it as custom metrics to [Datadog](https://datadoghq.com). It requires
at least a free Datadog account to send metrics to. 

## Dashboard

A public Datadog [dashboard built off of this data is available here.](https://p.datadoghq.com/sb/Qy0hiX-3acdca1f90d92099ce8c47cea4059955?theme=dark)

## Usage

If you would like to run your own copy, it's as simple as:

```
export DD_APP_KEY='<Datadog App Key here>
export DD_API_KEY='<Datadog API Key here>
pipenv run ./main.py
```
