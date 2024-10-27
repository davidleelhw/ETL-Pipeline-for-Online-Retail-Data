# ETL Pipeline for Online Retail Data

This project demonstrates an ETL (Extract, Transform, Load) pipeline that processes and loads the Online Retail dataset into a PostgreSQL database. The pipeline extracts data from a CSV file, cleanses and transforms it, and loads it into a structured database schema. Built with data engineering best practices, this project showcases skills in data processing, error handling, logging, and efficient database interactions.

[Online Retail Dataset from UCI Machine Learning Repository](https://archive.ics.uci.edu/dataset/352/online+retail)

## Project Overview

The ETL pipeline covers the following stages:
1. **Extract** - Loads raw data from a `.csv` file into a Pandas DataFrame.
2. **Transform** - Cleans and preprocesses the data, addressing missing values and ensuring data integrity.
3. **Load** - Inserts transformed data into a PostgreSQL database with a structured schema.

The pipeline is built with modularity in mind, employing techniques for handling missing data and mapping relationships, while incorporating basic transaction handling for data loading efficiency.

## Key Features

- **Data Transformation**: The transformation phase includes handling missing values, ensuring data types are correct, and performing custom mapping for missing entries.
- **Schema Design**: Designed a normalized database schema with four main tables: `customers`, `products`, `orders`, and `orderdetails`.
- **Logging**: Logs key events and errors to both the console and log files, making the pipeline traceable and easier to debug.
- **Modularity**: Each ETL phase is encapsulated in separate functions for easy maintenance and testing.

## Database Schema

The PostgreSQL database has the following schema:

- **`customers`**: Stores customer information, including:
  - `customerid`: INT, Primary Key
  - `country`: VARCHAR(100)

- **`products`**: Stores product details such as:
  - `stockcode`: VARCHAR(20), Primary Key
  - `description`: TEXT
  - `unitprice`: NUMERIC

- **`orders`**: Records each transaction with:
  - `invoiceno`: VARCHAR(20), Primary Key
  - `customerid`: INT, Foreign Key referencing `customers.customerid`
  - `invoicedate`: TIMESTAMP

- **`orderdetails`**: Holds individual items for each order, including:
  - `orderdetailid`: INTEGER, Primary Key (Auto-generated)
  - `invoiceno`: VARCHAR(20), Foreign Key referencing `orders.invoiceno`
  - `stockcode`: VARCHAR(20), Foreign Key referencing `products.stockcode`
  - `quantity`: INT
  - `unitprice`: NUMERIC

## Enhancements and Future Work

This project can be further enhanced with the following improvements:

- **Automated Scheduling**: Integrate with a workflow orchestration tool like [Apache Airflow](https://airflow.apache.org/) to automate ETL job scheduling, monitoring, and error handling.
- **Data Validation**: Implement data validation checks using tools like [Great Expectations](https://greatexpectations.io/) to ensure data integrity, detect anomalies, and create data quality reports.
- **Scalability Improvements**: Optimize batch inserts for higher efficiency and consider leveraging cloud services like [AWS Glue](https://aws.amazon.com/glue/) or [Amazon Redshift](https://aws.amazon.com/redshift/) for handling larger datasets.

Each of these additions would contribute to creating a robust, production-ready data pipeline.
