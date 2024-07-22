# Euro 2024 Data Pipeline

This repository contains the code for a data pipeline that loads data related to the Euro 2024 tournament into a Postgres database.  
The data is used to power a [Metabase Dashboard on the EURO 2024 tournament](https://www.metabase.com/gallery/uefa-euro-2024-stats-dashboard).  
The pipeline is built using the [dlt library](https://dlthub.com/docs/intro) and uses data from the [Sportmonks API](https://www.sportmonks.com/football-api/).

## Setup

To set up the project, you need to install the required dependencies. You can do this by running the following command:

```shell
pip install -r requirements.txt
```

Then, copy the `.env.example` file to `.env` and fill in the required environment variables.

```shell
cp .env.example .env
```

## Usage

You can run
the pipeline with the following command:

```shell
python sync.py [--full]
```

By default, the script will load the data for the current season. If you want to perform a full load of all the data, you can use the `--full` option.

We scheduled the ETL process using Github Actions. You'll find the code for that in the `.github/sync.yaml` file.
