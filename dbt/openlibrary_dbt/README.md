# Open Library Data Engineering Pipeline

## Overview
An end-to-end data engineering project that collects book data from the Open Library API, processes it through an automated ELT pipeline, and prepares it for SQL-based analysis.

## Architecture

**Open Library API → Python → Apache Airflow → PostgreSQL → dbt → Data Mart → SQL Analysis**

## Pipeline Workflow

1. Extraction — Python retrieves book data from the Open Library API.
2. Loading — Raw API responses are stored in PostgreSQL in JSONB format, along with extraction metadata.
3. Orchestration — Apache Airflow manages pipeline execution and scheduling in a Docker environment.
4. Transformation — dbt transforms raw data into structured staging models and an analytical data mart.
5. Data Quality — dbt tests validate data quality and model integrity.
6. SQL Analysis — PostgreSQL queries are executed through DBeaver to analyze publication trends, ebook availability, edition counts, and data completeness.

## Tech Stack

- Programming: Python, SQL
- Orchestration: Apache Airflow
- Database: PostgreSQL
- Transformation & Testing: dbt
- Environment: Docker
- SQL Client: DBeaver
- Data Source: Open Library API

## Project Structure

- `dags/` — Airflow DAGs for pipeline orchestration.
- dbt project directory — staging models, data mart transformations, and data quality tests.
- `sql/analysis_queries.sql` — SQL queries for data analysis.

## Outcome

This project demonstrates an automated ELT workflow, structured data transformation, data quality validation, and SQL-based analysis using an analytical data mart.

