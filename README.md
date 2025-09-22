# OpenAQ
OpenAQ – Air Quality ETL

Personal project to practice data engineering. The pipeline fetches data from the OpenAQ API handling rate limit, processes it, and loads it into an SQLite database. It supports incremental loading with logging so that runs can resume after errors.

## Stack

* Python
* SQLite
* Docker
* (Planned) Airflow for orchestration
* (Planned / Maybe) Great Expectations for data checks
* (Planned) Streamlit for a simple dashboard

## Usage



Local run:

``python main.py``


Docker:

``docker-compose up --build``

## Status

* ETL working end-to-end

* Incremental load + logging done

### In progress: 
* splitting ETL into smaller tasks to prepare for Airflow integration

### Planned:
* Airflow orchestration
* Data quality checks
* Streamlit dashboard
