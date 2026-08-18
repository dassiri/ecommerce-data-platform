# E-Commerce Data Platform

An end-to-end e-commerce data platform built with PostgreSQL, dbt, and layered data warehouse architecture.

## Project Overview

This project demonstrates a production-oriented data engineering workflow for ingesting, storing, transforming, testing, and serving e-commerce data.

## Architecture

The platform follows a layered data warehouse architecture:

- Raw Data Layer
- Staging Layer
- Intermediate Layer
- Marts / Analytics Layer

## Technology Stack

- Python
- PostgreSQL
- dbt
- SQL
- Git & GitHub
- Data Quality Testing

## Project Structure

```text
ecommerce-data-platform/
├── data/
│   └── raw_source/
├── database/
├── docs/
├── ingestion/
├── tests/
├── warehouse/
│   └── dbt/
├── .env.example
├── .gitignore
├── pyproject.toml
├── requirements.txt
├── requirements-dbt.txt
└── requirements-dev.txt
