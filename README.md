# ETL Project: Airbnb Data Loading

This project is an ETL (Extract, Transform, Load) process aimed at loading and transforming Airbnb data for subsequent analysis. In this initial stage, we focus on loading data from a CSV file into a PostgreSQL database.

## Project Structure

The project is organized as follows:

Proyecto_ETL:           .gitignore, credentials.json, README.md

Proyecto_ETL/data:      Airbnb_Open_Data.csv

Proyecto_ETL/Notebooks: 001_DataLoad.ipynb, 002_EDA.ipynb, 003_CleanData.ipynb

### Description of Files and Folders

- **.gitignore**: File specifying which files and folders should be ignored by Git. In this case, the `credentials.json` file is ignored for security reasons.
- **credentials.json**: File containing the credentials for accessing the PostgreSQL database.
- **README.md**: This file, providing an overview of the project.
- **data/**: Folder containing the CSV file with Airbnb data.
- **Notebooks/**: Folder containing the Jupyter notebooks used in the project.

### Install Dependencies:
   ```bash
   pip install pandas sqlalchemy psycopg2 matplotlib seaborn 
   ```

## Notebook 001_DataLoad.ipynb

This notebook handles the initial loading of data from the CSV file `Airbnb_Open_Data.csv` into a PostgreSQL database.

### Steps Performed in the Notebook

1. **Credentials Upload**: The database credentials are uploaded from the `credentials.json` file.

2. **Dataset upload**: The CSV file `Airbnb_Open_Data.csv` is uploaded into a Pandas DataFrame.

3. **Database Creation**: Check if the `airbnb` database exists in PostgreSQL. If it does not exist, it is created.

4. **Upload Data to the Database**: A table called `airbnb_data` is created in the database and the data from the DataFrame is uploaded to this table.

5. **Data Verification**: Simple queries are performed to verify that the data has been correctly loaded into the database.

### Instructions for Use

1. **Clone the repository**:
   ```bash
   git clone https://github.com/tu_usuario/Proyecto_ETL.git
   cd Proyecto_ETL

Configure Credentials:
a `credentials.json` file must be created with the following content:
  ```javascript
  {
  "user": "user",
  "password": "password",
  "host": "host",
  "port": "port", 
  "database": "database"
  }
  ```

After you have created it, make sure that the credentials.json file is correctly configured with your PostgreSQL credentials.

### Run the Notebook:
Open the notebook 001_DataLoad.ipynb in Jupyter and run the cells in order.

# ETL Project: Exploratory Data Analysis (EDA)

In this stage of the project, we focus on EDA using the `002_EDA.ipynb` notebook. This notebook aims to explore and visualise the Airbnb data loaded into the PostgreSQL database to identify patterns, trends and potential problems in the data.

## Notebook 002_EDA.ipynb

This notebook performs an exploratory analysis of the Airbnb data, including identifying null values, duplicates, and visualising distributions and relationships between variables.

### Steps Performed in the Notebook

1. **Data Upload**: Connect to the PostgreSQL database and upload data from the `airbnb_data` table.

2. **Initial Scan**:
   - The size of the dataset (number of rows and columns) is checked.
   - Duplicate rows are identified.
   - Calculate null values and their percentage per column.


3. **Data Type Analysis**: The data types of each column are checked to ensure that they are correct.

4. **Data Visualisation**:
   - Price distribution.
   - Relationship between prices and minimum nights.
   - Distribution of service rates.
   - Detection of outliers in prices and service rates.
   - Distribution of room types.
   - Distribution of publications by year of construction.
   - Percentage of verified vs. unverified hosts.
   - Number of postings by neighbourhood group.
   - Percentage of each type of cancellation policy.
   - Distribution of properties with instant booking.

### Run the Notebook:
Open notebook 002_EDA.ipynb in Jupyter and run the cells in order.

# ETL Project: Data cleansing

In this stage of the project, we focus on cleaning and transforming the data using the `003_CleanData.ipynb` notebook. This notebook aims to prepare the data for further analysis by removing null values, correcting errors and transforming columns to make them more useful.

## Notebook 003_CleanData.ipynb

This notebook performs cleanup and transformation of Airbnb data stored in the PostgreSQL database. It focuses on creating a new table (`airbnb_EDA`), renaming columns, removing unnecessary columns, handling null values and correcting errors in the data.

### Steps Performed in the Notebook

1. **Load Data**:
   - Connect to the PostgreSQL database and load data from the `airbnb_data` table.

2. **Creation of a New Table (`airbnb_EDA`)**:
   - A new table called `airbnb_EDA` is created with the same information as the original table (`airbnb_data`).
   - Check that the new table has been created correctly and that it contains the same number of records as the original table.

3. **Column renaming**:
   - Columns that contain spaces in their names are renamed to make them easier to handle in SQL queries.
   - Example: `host id` becomes `host_id`, `minimum nights` becomes `minimum_nights`, etc.

4. **Removal of Unnecessary Columns**:
   - Columns that are not relevant to the analysis are removed, such as `host_name`, `lat`, `long`, `country_code`, `country`, `neighbourhood`, and `house_rules`.

5. **Null Value Handling**:
   - Null values in text columns are replaced with ``not fill``.
   - Replace null values in numeric columns with `-1`.
   - Transform the `last_review` column to convert dates to a numeric format (`YYYYYMMDD`). Null dates are replaced with `99999999`.

6. **Correction of Data Errors**:
   - Errors in the `neighbourhood_group` column are corrected, such as `‘brookln’` and `‘manhatan’`, which are changed to `‘Brooklyn’` and `‘Manhattan’`, respectively.

6. **Run the Notebook**:
Open notebook 003_CleanData.ipynb in Jupyter and run the cells in order.
