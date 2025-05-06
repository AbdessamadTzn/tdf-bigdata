# Tour de France Data Pipeline Project

This project aims to build a complete data pipeline using real-world cycling data from the Tour de France. It showcases the use of modern cloud and big data technologies to collect, process, analyze, and visualize data efficiently.

## Project Objectives

- Automate data ingestion and transformation from CSV sources
- Process large-scale data using Apache Beam and Google Dataflow
- Store structured data in BigQuery for scalable analytics
- Visualize insights through dashboards in Looker Studio (formerly Data Studio)
- Ensure code quality and security with CodeQL analysis on GitHub

---

## ⚙Technologies Used

| Tool              | Purpose |
|-------------------|---------|
| Google Cloud Platform (GCP) | Cloud infrastructure |
| Google Storage    | Store input data (CSV files) |
| Apache Beam       | Data transformation & pipeline logic |
| Dataflow          | Beam pipeline execution engine |
| BigQuery          | Scalable data warehouse for storage and SQL analytics |
| Looker Studio     | Data visualization dashboards |
| CodeQL            | Security and quality analysis of the codebase |

---

## Project Workflow

### 1. Data Preparation

Raw Tour de France data was gathered and stored in Google Cloud Storage (GCS) in CSV format. This serves as the input for the pipeline.

### 2. Pipeline Implementation

The data pipeline was implemented using **Apache Beam**. It reads data from GCS, applies transformations, and writes the cleaned output to BigQuery. The pipeline is designed to run on the **Google Dataflow** service for scalability.

### 3. Deployment to GCP

Using the `gcloud` CLI, credentials were authenticated, the project was set up, and the Beam pipeline was deployed to Dataflow for execution.

### 4. Data Storage in BigQuery

Transformed data was written to a BigQuery dataset. This enables fast querying using SQL and integrates well with visualization tools.

### 5. Dashboard Creation

A custom dashboard was created using **Looker Studio**. It connects directly to the BigQuery table and visualizes key statistics and trends in the Tour de France history (e.g. top riders, stages, average speeds).

### 6. Code Security with CodeQL

To ensure secure and high-quality code, **CodeQL** was configured in GitHub Actions. It automatically scans the codebase on every commit and pull request, identifying potential security issues and code smells.

---

## ✅ Outcomes

- Built a complete ETL pipeline using modern tools
- Gained experience in cloud-based data engineering
- Visualized meaningful insights from historical sports data
- Applied best practices with GitHub Actions and CodeQL

---

## Project Structure

- `tdf_pipeline.py`: Main Beam pipeline script
- `data/`: Raw data files (hosted on GCS)
- `dashboard/`: Screenshots or links to the Looker Studio dashboard
- `.github/workflows/codeql.yml`: CodeQL security workflow (optional)

---

## Dashboard Preview
[Live dashboard](https://lookerstudio.google.com/reporting/9d20e54d-4615-4b29-a821-d0fd945236b7)
---
