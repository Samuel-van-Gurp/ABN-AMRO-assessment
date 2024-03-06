# Customer Data Processing Script

This script combines two separate datasets: one containing information about the clients and the other containing information about their financial details.

## Implementation 

The script consists of several functions and a main function that orchestrates the entire data processing pipeline:

- `load_and_clean_data`: This function loads a CSV file into a Spark DataFrame and performs data cleaning tasks like dropping specified columns and renaming columns.

- `filter_data`: This function filters a DataFrame based on specified conditions provided as column-value pairs.

- `join_data`: This function joins two DataFrames based on a specified join key, in this case, customer identification number.

- `save_data`: This function saves the DataFrame to a CSV file.

- `main`: The main function orchestrates the entire data processing pipeline by calling the above functions in sequence. It loads, cleans, filters, joins, and saves two datasets.

## Requirements 

The project was developed in Python 3.10 using the PySpark package.
