# Project ETL with Apache Airflow and Kafka

> A modular **ETL** (Extract–Transform–Load) solution combining batch processing and real-time streaming via **Kafka**, orchestrated with **Apache Airflow**, and data quality validation using **Great Expectations**.

---

## 📖 Table of Contents

1. [Overview](#overview)
2. [Repository Structure](#repository-structure)
3. [Installation & Configuration](#installation--configuration)

   1. [Airflow Virtual Environment](#airflow-virtual-environment)
   2. [Dependencies](#dependencies)
4. [Docker Compose Deployment](#docker-compose-deployment)
5. [Apache Airflow](#apache-airflow)

   1. [Main DAGs](#main-dags)
6. [Data Validation (Great Expectations)](#data-validation–great-expectations)
7. [Batch Processing (Batch ETL)](#batch-processing–batch-etl)
8. [Real-Time Processing (Kafka Streaming)](#real-time-processing–kafka-streaming)
9. [Jupyter Notebooks](#jupyter-notebooks)
10. [Cache Cleaning](#cache-cleaning)

---

## 📌 Overview

This project implements a complete data pipeline that:

* **Extract**: Fetches raw data from various sources (CSV, API, Kafka).
* **Transform**: Cleans and enriches data using business rules and quality checks.
* **Load**: Inserts processed data into a dimensional model in **PostgreSQL**.
* **Orchestrate**: Manages task dependencies and scheduling with **Apache Airflow**.
* **Stream**: Processes data in real time using **Kafka** (producer/consumer).
* **Validate**: Verifies data quality with **Great Expectations**.

---

## 🗂 Repository Structure

```text
.
├── .gitignore
├── docker-compose.yml
├── clean_pycache.sh
├── create_airflow_venv.txt
├── dashboard_project01_final_.pdf
├── md_03_project.pdf
├── requirements.txt
├── src/
│   ├── producer.py
│   ├── consumer.py
│   ├── etl_utils.py
│   └── …
├── src_nico/
├── scripts_ge/
│   └── run_validations.py
├── great_expectations/
│   ├── great_expectations.yml
│   └── suites/
├── airflow/
│   └── dags/
│       ├── etl_dag.py
│       └── kafka_streaming_dag.py
├── Notebooks/
│   ├── 01_DataLoad.ipynb
│   ├── 02_EDA.ipynb
│   └── 03_StreamEDA.ipynb
├── data/
└── .vscode/
```

* **`.gitignore`**: Files and folders to ignore (envs, caches, credentials).
* **`docker-compose.yml`**: Defines Docker services (Zookeeper, Kafka, PostgreSQL, Airflow).
* **`clean_pycache.sh`**: Bash script to remove `__pycache__` directories and `.pyc` files.
* **`create_airflow_venv.txt`**: Steps to create and activate a virtual environment for Airflow.
* **`requirements.txt`**: Python dependencies (`pandas`, `sqlalchemy`, `kafka-python`, `apache-airflow`, `great_expectations`, etc.).
* **`src/`**: Main ETL and streaming code (producer, consumer, utilities).
* **`src_nico/`**: Experimental or alternative ETL modules.
* **`scripts_ge/`**: Scripts to run Great Expectations validations.
* **`great_expectations/`**: GE configuration, suites, and checkpoints.
* **`airflow/dags/`**: DAG definitions for batch and streaming workflows.
* **`Notebooks/`**: Jupyter Notebooks for EDA and pipeline testing.
* **`data/`**: Example input data (CSV, JSON).
* **`.vscode/`**: VS Code settings (linter, debugger, workspace).

---
## Create a folder named env/ with a file called .env:

```bashr
{
  "user": "your_user",
  "password": "your_password",
  "host": "your_host",
  "port": "your_port",
  "database": "airbnb"
}
```
## ⚙️ Installation & Configuration

### Airflow Virtual Environment

```bash
python3 -m venv venv_airflow
source venv_airflow/bin/activate
```

### Dependencies

With the virtual environment active, install Airflow and other packages:

```bash
pip install "apache-airflow==2.X.X" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.X.X/constraints-3.8.txt"

pip install -r requirements.txt
```

---

## 🐳 Docker Compose Deployment

Bring up all services:

```bash
docker-compose up -d
```

* The `etl-network` connects Kafka, PostgreSQL, and Airflow.
* Volumes persist database data and logs.

---

## 🛩 Apache Airflow

1. **Initialize the metadata database**

   ```bash
   airflow db init
   ```
2. **Create an admin user**

   ```bash
   airflow users create \
     --username admin \
     --firstname Admin \
     --lastname User \
     --role Admin \
     --email admin@example.com
   ```
3. **Start scheduler and webserver**

   ```bash
   airflow scheduler &
   airflow webserver --port 8080 &
   ```

### Main DAGs

* **`etl_dag.py`**
  Orchestrates batch extraction, transformation, and loading into the dimensional model.

* **`kafka_streaming_dag.py`**
  Manages continuous consumption from Kafka and incremental loading.

---

## ✅ Data Validation (Great Expectations)

* **Configuration**: `great_expectations/great_expectations.yml` and expectation suites.
* **Runner**: `scripts_ge/run_validations.py` executes all suites and generates an HTML report.

---

## 🗃 Batch Processing (Batch ETL)

Defined in `airflow/dags/etl_dag.py`, includes:

1. **Extract**: Reads CSV/JSON files from `data/`.
2. **Transform**: Cleans, normalizes, and enriches data.
3. **Load**: Inserts records into dimension and fact tables in PostgreSQL.

---

## 🚀 Real-Time Processing (Kafka Streaming)

Core modules in `src/`:

* **`producer.py`**: Publishes messages to `etl_topic`.
* **`consumer.py`**: Consumes messages, transforms, and loads incrementally.
* **`etl_utils.py`**: Shared utilities for serialization, validation, and DB connections.

---

## 📓 Jupyter Notebooks

* **`01_DataLoad.ipynb`** – Extraction and loading demos.
* **`02_EDA.ipynb`** – Batch data exploratory analysis.
* **`03_StreamEDA.ipynb`** – Real-time visualization from Kafka.

---

## 🧹 Cache Cleaning

To remove compiled files:

```bash
bash clean_pycache.sh
```

## Authors
### `Jakcobo`
GitHub: github.com/Jakcobo
### `y4xulSC`
GitHub: github.com/y4xulSC
### `Mrsasayo`
GitHub: github.com/Mrsasayo
