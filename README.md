# luftdaten
Collect your sensor's luftdaten archive data and plot.<br>
Based on particulate counts for PM2.5 and PM10 using a PMS5003 sensor but it may work for other sensor data.
Developed on Python 3.7

## Features

* Pulls CSV data if it doesn't exist locally
* Plots for several date ranges
* Particulate counts over a time series
* Average counts per day
* Average counts per hour to show daily trend
* Pushes HTML to S3 if configured (assumes a working AWS config on the executing machine)
* Will open up browser and show plots locally if "--show" is supplied as an argument

## Installation

* Create a config.json file to suit your requirements (see example)
* Edit Pipfile to suit your requirements and python version 
* Create a virtualenv or use pipenv python environment

e.g.

```bash
cd luftdaten
pipenv install
pipenv run python luftdaten.py --show
```



