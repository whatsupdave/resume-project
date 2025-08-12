# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Context

This is a **resume project** for aspiring Junior Data Engineer role. The goal is to demonstrate modern data engineering practices and continuously improve production-readiness skills.

**Mentoring Approach**: Act as a senior data engineer mentor. Provide guidance and direction rather than complete solutions. Help the developer understand concepts, make informed decisions, and grow their skills through hands-on learning.

## Project Overview

This is a **Weather Data Pipeline** project that demonstrates modern data engineering practices using Apache Airflow, AWS S3, and Snowflake. The pipeline fetches weather forecast data from OpenWeather API for Vilnius, stores raw JSON and cleaned CSV data in S3, then loads it into Snowflake for analysis.

## Architecture

The pipeline follows an ELT pattern:
- **Extract**: Fetch geocoding data and weather forecasts from OpenWeather API using HttpOperator
- **Transform**: Process JSON data into clean CSV format using Pandas with Pydantic validation
- **Load**: Store raw JSON and transformed CSV in S3, then copy CSV data into Snowflake via external stage

Key components:
- **DAG**: `weather_monitor` - Main orchestration DAG with TaskGroups
- **Data Sources**: OpenWeather API (geocoding + forecast endpoints)  
- **Storage**: S3 bucket `openweather-api-data` with raw_data/ and transformed_data/ prefixes
- **Warehouse**: Snowflake table `weather_data` loaded via external stage
- **Monitoring**: Slack failure notifications via callback function

## Development Commands

### Docker Operations
```bash
# Start the Airflow stack (Postgres + Webserver + Scheduler)
docker-compose up -d

# Stop the stack
docker-compose down

# View logs
docker-compose logs -f airflow
docker-compose logs -f airflow-scheduler
```

### Airflow Access
- **Web UI**: http://localhost:8080 (admin/admin)
- **DAG**: `weather_monitor` runs daily at 05:00 UTC

### Environment Setup
- Copy `.env` file with required credentials:
  - AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY  
  - SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
  - AIRFLOW_FERNET_KEY for encryption
- Configure Airflow connections:
  - `openweathermap_default`: HTTP connection with API key in password field
  - `aws_default`: AWS S3 connection
  - `snowflake_conn_id`: Snowflake connection

## Code Structure

### Main DAG (`airflow/dags/weather_api.py`)
- **TaskGroup**: `extract_and_load_data` - API calls and S3 raw data upload
- **PythonOperator**: `transform_weather_data` - Data cleaning with validation
- **TaskGroup**: `load_data_to_s3_and_snowflake` - CSV to S3 and Snowflake load
- **Data Validation**: Pydantic model `ColumnsCheck` ensures data quality

### Utilities (`airflow/utils/slack_helper.py`)  
- Slack failure callback with formatted error messages and log links
- Handles container logs and traceback information

### Testing (`airflow/tests/test_weather_api.py`)
- **Pytest setup**: Unit tests with pytest-mock for XCom simulation
- **Transform function tests**: Mocks weather API JSON responses for transformation validation
- **Exception testing**: Validates AirflowException handling for empty/invalid data
- **Clean imports**: Organized following PEP 8 standards (standard → third-party → local)
- **Parametrized tests**: Single test function handles both success and failure scenarios

### File Naming Convention
- Raw files: `raw_data/{{ data_interval_start | ds }}_{city}.json`
- Transformed files: `transformed_data/{{ data_interval_start | ds }}_{city}.csv`

## Key Development Notes

- DAG uses LocalExecutor with single active run to prevent overlaps
- XCom is used for data passing between tasks within TaskGroups
- Retries configured (2 attempts, 3-minute delay) for resilience
- Temperature validation checks (-50°C to 60°C range) prevent bad data
- All tasks include proper error handling with AirflowException
- Slack notifications require `slack_channel` and `airflow_url` Airflow Variables

## Areas for Production Enhancement

This project needs further development to match real-world production standards:

### Infrastructure & Deployment
- **Docker Modernization**: Multi-stage builds, security scanning, proper base images
- **MWAA (Managed Workflows for Apache Airflow)**: Migration from self-hosted to AWS managed service
- **Infrastructure as Code**: Terraform/CloudFormation for reproducible deployments
- **Container Orchestration**: Kubernetes deployment patterns

### CI/CD Pipeline - **PRODUCTION READY** ✅
- **Automated Testing**: ✅ Unit tests with pytest + automated CI workflow  
- **Code Quality**: ✅ Pylint with 9.0+ threshold and Airflow-specific configuration
- **Security Scanning**: ✅ GitHub CodeQL for vulnerability detection
- **Dependency Management**: ✅ Airflow constraints for stable, conflict-free dependencies
- **Quality Gates**: ✅ All PRs require passing tests, code quality, and security checks
- **GitHub Actions**: ✅ Separated workflows for testing and linting with proper dependency management
- **DAG Validation**: ✅ Syntax checking, import testing, and configuration validation
- **Professional Setup**: ✅ .pylintrc, requirements.txt with constraints, proper project structure

### Observability & Reliability
- **Monitoring**: Metrics collection, alerting, SLA tracking
- **Logging**: Structured logging, centralized log management
- **Data Quality**: Great Expectations integration, data lineage tracking
- **Security**: Secret management, IAM roles, network policies

### Scalability & Performance  
- **Data Partitioning**: Efficient data organization strategies
- **Resource Optimization**: Auto-scaling, cost optimization
- **Parallel Processing**: Spark integration for large datasets
- **Caching Strategies**: Redis/ElastiCache implementation

### Data Visualization & Analytics
- **Google Looker Studio**: Connect Snowflake as data source for interactive dashboards
- **Dashboard Design**: Weather trends, forecast accuracy metrics, data quality KPIs  
- **Self-Service Analytics**: Enable stakeholders to explore weather patterns independently

When working on enhancements, guide the developer to:
1. Research industry best practices first
2. Understand the "why" before implementing the "how" 
3. Start with MVP implementations and iterate
4. Consider trade-offs and architectural decisions
5. Document learning outcomes and decisions made