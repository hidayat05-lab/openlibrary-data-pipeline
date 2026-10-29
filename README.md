# Open Library ELT Data Pipeline

An end-to-end data engineering project that extracts book metadata from the Open Library API, orchestrates data workflows with Apache Airflow, stores raw data in PostgreSQL, transforms data using dbt, and prepares curated datasets for SQL-based analysis.

## Overview

This project demonstrates a practical ELT (Extract, Load, Transform) workflow using modern data engineering tools.

The pipeline collects book metadata from the Open Library API, stores the source data in a raw PostgreSQL layer, and transforms it into structured analytical datasets using dbt. Apache Airflow orchestrates the workflow, while dbt tests help validate data quality.

The resulting data mart is designed to support further exploration and analysis using SQL in DBeaver.

### Project Objectives

- Build an automated pipeline for extracting book metadata from a public API.
- Orchestrate and schedule data workflows using Apache Airflow.
- Store raw source data in PostgreSQL while preserving the original JSON payload.
- Transform and deduplicate records using dbt.
- Apply data quality tests to improve data reliability.
- Develop SQL queries to explore publication trends, edition counts, and digital book availability.

## Architecture

```text
Open Library Search API
          |
          v
    Python Extraction
          |
          v
 Apache Airflow
 (Orchestration)
          |
          v
   PostgreSQL RAW
 (JSONB + Metadata)
          |
          v
       dbt STAGING
 (Cleaning & Standardization)
          |
          v
      dbt DATA MART
 (Deduplication & Business Logic)
          |
          v
    Data Quality Tests
          |
          v
    SQL Data Analysis
        (DBeaver)
```

**Technologies:** Python, Apache Airflow, PostgreSQL, dbt, Docker, SQL, and DBeaver.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Extracts book metadata from the Open Library API |
| Apache Airflow | Orchestrates and schedules pipeline tasks |
| PostgreSQL | Stores raw and transformed datasets |
| JSONB | Preserves raw API responses in a queryable format |
| dbt | Performs data transformation, modeling, and testing |
| Docker | Provides a containerized development environment |
| SQL | Supports data validation and analytical queries |
| DBeaver | Provides a database interface for SQL development and analysis |

## Pipeline Workflow

### 1. Data Extraction

Python retrieves book metadata from the Open Library Search API.

The extracted attributes can include book titles, authors, publication years, languages, edition counts, ebook access information, and full-text availability, depending on the fields returned by the API.

### 2. Data Loading — RAW Layer

The extracted API responses are loaded into PostgreSQL in a raw data layer.

The raw table, `raw.openlibrary_books`, stores the source payload as JSONB alongside ingestion metadata, including a batch identifier, source information, and extraction timestamp.

Preserving the original payload provides traceability and supports downstream transformations.

### 3. Workflow Orchestration

Apache Airflow manages the pipeline workflow and its task dependencies.

The pipeline runs in a Docker-based environment, allowing its components to operate in a consistent and reproducible setup.

Airflow provides a centralized interface for monitoring task execution and troubleshooting pipeline failures.

### 4. Data Transformation with dbt

dbt transforms raw book metadata into structured analytical models.

The transformation process includes:

- Extracting relevant attributes from JSON data.
- Standardizing fields for analytical use.
- Deduplicating records using `book_key`.
- Selecting the latest available record using extraction timestamps and record identifiers.
- Creating derived dimensions for publication periods and edition groups.
- Deriving ebook availability and full-text status classifications.

The transformation layer follows a staging-to-mart approach.

| Layer | Model | Purpose |
|---|---|---|
| RAW | `raw.openlibrary_books` | Stores source API payloads and ingestion metadata |
| STAGING | `analytics.stg_openlibrary_books` | Structures and standardizes extracted attributes |
| DATA MART | `analytics.mart_books` | Provides deduplicated, analysis-ready book records |

The staging model is implemented as a view, while the data mart is materialized as a table.

### 5. Data Quality Testing

dbt tests are used to validate the transformed data and identify potential quality issues.

The testing workflow supports checks for model integrity and key data constraints, helping detect problems before the data is used for analysis.

The latest recorded test run completed with **12 passing tests and no warnings or errors**. This reflects that specific test run, rather than a guarantee about every future execution.

### 6. SQL Analysis

The data mart can be explored using SQL in DBeaver, without requiring a separate business intelligence platform.

The analysis queries are designed to investigate:

- Total records and unique book identifiers.
- Missing titles, publication years, and edition counts.
- Book distribution by publication decade.
- Ebook availability and full-text status.
- Books with the highest edition counts.
- Aggregated publication-period statistics.

These queries establish a foundation for understanding the dataset. Analytical findings should be documented only after the queries have been executed and their results reviewed.

## Data Mart Schema

The `analytics.mart_books` table contains curated book-level records.

| Column | Description |
|---|---|
| `book_key` | Open Library book identifier used for deduplication |
| `title` | Book title |
| `author_name` | Author name or names, when available |
| `author_key` | Open Library author identifier or identifiers |
| `first_publish_year` | First publication year, when available |
| `publication_period` | Derived publication-period category |
| `publication_decade` | Derived publication decade |
| `language` | Language information, when available |
| `edition_count` | Number of editions reported by the source |
| `edition_group` | Derived category based on edition count |
| `ebook_access` | Source-provided ebook access classification |
| `ebook_available` | Derived ebook availability indicator |
| `has_fulltext` | Source-provided full-text availability indicator |
| `fulltext_status` | Derived full-text status classification |
| `cover_i` | Open Library cover identifier, when available |
| `cover_edition_key` | Cover edition identifier, when available |
| `source` | Source information |
| `extracted_at` | Extraction timestamp |

Some fields may be null or unavailable in the original API response.

## Data Quality and Deduplication

A central transformation rule is the selection of the latest record for each `book_key`.

The deduplication logic uses a window function:

```sql
ROW_NUMBER() OVER (
    PARTITION BY book_key
    ORDER BY extracted_at DESC, id DESC
)
```

The most recent record is selected for the curated data mart, helping prevent multiple extracted versions of the same book from appearing as separate book-level records.

Derived classifications make the data easier to aggregate and analyze, while dbt tests provide an additional validation layer.

## Project Structure

The repository contains the main components of the pipeline:

```text
openlibrary-data-pipeline/
├── dags/                     # Airflow DAG definitions
├── dbt/
│   └── openlibrary_dbt/      # dbt project and transformation models
├── sql/                      # SQL analysis queries
├── tests/                    # Pipeline-related tests
├── Dockerfile                # Container image configuration
├── docker-compose.override.yml
├── packages.txt
├── requirements.txt
├── .dockerignore
├── .gitignore
└── README.md
```

The exact files within each directory may evolve as the project develops.

## Running the Project

The pipeline is designed for a local Docker-based development environment.

Before running the project, ensure that Docker Desktop, the Astro CLI, and the required Python and dbt dependencies are installed and configured.

At a high level, the workflow is:

1. Start the local Airflow development environment.
2. Confirm that the required PostgreSQL service is running.
3. Trigger or wait for the configured Airflow pipeline.
4. Verify that raw records have been loaded into PostgreSQL.
5. Run the dbt transformation models.
6. Execute the dbt tests.
7. Connect to PostgreSQL through DBeaver and run the SQL analysis queries.

Database credentials and other environment-specific settings should be configured locally and must not be committed to the repository.

## Current Scope and Future Improvements

The project establishes an end-to-end foundation for API ingestion, orchestration, transformation, and analytical querying.

Potential future improvements include:

- Adding incremental extraction and loading strategies.
- Strengthening schema validation and source-to-target reconciliation.
- Expanding automated data quality checks.
- Improving pipeline retry and failure-notification handling.
- Documenting analytical findings using verified SQL results.
- Adding a dashboard when a suitable visualization environment is available.

## Key Takeaways

This project demonstrates how multiple data engineering components can work together in a single workflow:

- **Ingestion:** Python and a public API.
- **Orchestration:** Apache Airflow.
- **Storage:** PostgreSQL and JSONB.
- **Transformation:** dbt staging models and a curated data mart.
- **Validation:** Automated dbt data quality tests.
- **Analysis:** SQL queries executed through DBeaver.

The repository serves as a practical learning project focused on building and understanding a complete ELT pipeline.

## Data Source

- [Open Library Search API](https://openlibrary.org/dev/docs/api/search)

## Author

**Rizki Hidayat Nasution**

Aspiring Data Engineer | SQL | Python | Apache Airflow | PostgreSQL | dbt
