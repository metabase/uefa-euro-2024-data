# Euro 2024 Data Pipeline

This repository contains the code for a data pipeline that loads data related to the Euro 2024 tournament into a Postgres database. The pipeline is built using the dlt library.

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
