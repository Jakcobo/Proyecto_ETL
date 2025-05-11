# ETL Project: Airbnb Data Pipeline with Apache Airflow
This project implements a full ETL (Extract, Transform, Load) pipeline for Airbnb data and Foursquare Places API data using Python, Jupyter notebooks, and Apache Airflow. The data is transformed, cleaned, analyzed, and loaded into a PostgreSQL database for further exploration and modeling.

## Project Structure

```
Proyecto_ETL/
│
├── airflow/                  # Apache Airflow DAGs and tasks
│   └── dags/
│       ├── dag.py            # DAG orchestration
│       └── task_etl.py       # Task functions for ETL
│
├── data/
│   └── Airbnb_Open_Data.csv  # Raw Airbnb dataset
│
├── database/
│   ├── db.py                 # PostgreSQL connection and database creation
│   └── model.py              # Database schema and model setup
│
├── extract/
│   └── extract.py            # Data extraction logic
│
├── load/
│   ├── load_data.py          # Load data into PostgreSQL
│   └── model_dimensional.py  # Dimensional model logic
│
├── transform/
│   ├── api_clean.py          # Data cleaning from APIs
│   └── dataset_clean.py      # Airbnb data cleaning scripts
│
├── Notebooks/
│   ├── 001_DataLoad.ipynb    # Load Airbnb data into PostgreSQL
│   ├── 002_EDA.ipynb         # Exploratory Data Analysis
│   ├── 003_CleanData.ipynb   # Data cleaning and transformation
│   └── 004_EDA_Api.ipynb     # EDA on Foursquare API data
│
├── env/
│   └── .env                  # PostgreSQL credentials (ignored in Git)
│
├── .gitignore
├── credentials.json          # (Ignored) PostgreSQL config
├── configuration_venv_airflow.txt
├── delete_pycache.sh
├── model_dimensional.pdf     # Star schema diagram
├── requeriremntes.txt
└── README.md                 # You are here
```
## Installation
Install the required libraries:

bash
pip install pandas==2.1.4 numpy==1.26.4 sqlalchemy psycopg2 matplotlib seaborn
Create a file named credentials.json:

```
{
  "user": "your_user",
  "password": "your_password",
  "host": "your_host",
  "port": "your_port",
  "database": "airbnb"
}
```
## Apache Airflow Pipeline
This DAG automates the ETL pipeline using Apache Airflow. The graph below shows the execution flow:


DAG Task Flow:
extract_data_task: Reads Airbnb data from Airbnb_Open_Data.csv.

- **clean_data_task:** Cleans the raw data using custom transformation logic.

- **load_cleaned_data_task:** Loads the cleaned data into the PostgreSQL database.

- **create_model_task:** Sets up the dimensional model (star schema).

- **insert_data_to_model_task:** Inserts the final transformed data into the fact and dimension tables.

Each task is defined using Airflow’s **@task** decorator for modularity and reusability.

## Jupyter Notebooks
### 001_DataLoad.ipynb – Initial Data Load
Loads the raw CSV file.

Creates the airbnb PostgreSQL database.

Uploads the data into the airbnb_data table.

Verifies the data with queries.

### 002_EDA.ipynb – Exploratory Data Analysis
Checks nulls, duplicates, data types.

Visualizes key trends:

Price distribution.

Minimum nights vs price.

Room types.

Host verification.

Neighborhoods and policies.

### 003_CleanData.ipynb – Data Cleaning & Transformation
Creates a new table airbnb_EDA.

Renames columns for SQL compatibility.

Removes irrelevant columns.

Handles nulls:

**Text:** replaced with ``"not fill"``.

**Numbers:** replaced with ``-1``.

**Dates:** transformed or filled with ``99999999``.

Fixes typos in ``neighbourhood_group``.

### 004_EDA_Api.ipynb – Foursquare Places API EDA
Fetches venue data for NYC boroughs.

Normalizes categories with fuzzy matching.

Cleans and exports as CSV.

### Visualizes:

Venue distributions (bar chart, heatmap).

Interactive map (Folium).

Top 5 categories by borough.

## Dimensional Modeling
A star schema is implemented for Airbnb data using the following entities:

**Fact Table:** Reservations (with price, nights, and dates).

**Dimensions:** ``Hosts``, ``room types``, ``location``, and ``cancellation policies``.

## See model_dimensional.pdf for full schema.

## Development Tips
Use delete_pycache.sh to clean .pyc and __pycache__ folders.

All secrets are managed through .env and .gitignore for safety.

Install Airflow and activate the virtual environment using configuration_venv_airflow.txt.

## Future Improvements
Integrate a dashboard (e.g., using Streamlit).

Schedule the pipeline for daily updates.

Add ML predictions for dynamic pricing.

## Authors
### `Jakcobo`
GitHub: github.com/Jakcobo
### `y4xulSC`
GitHub: github.com/y4xulSC
### `Mrsasayo`
GitHub: github.com/Mrsasayo
