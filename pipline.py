from pyspark.sql import SparkSession
import logging

# Set the logging level
logging.basicConfig(level=logging.INFO)

# Create a Spark session
spark = SparkSession.builder.appName("costumer_data").getOrCreate()

def load_and_clean_data(path_to_data: str,
                        columns_to_drop=[],
                        column_rename={}):
    """
    Function load a CSV file into Spark DataFrames and cleans the data, by dropping columns and renaming columns.

    :arg path_to_data: str
        The file path to the dataset (CSV file).
    :arg columns_to_drop: list, optional
        A list of column names to drop from the dataset. Default is [].
    :arg column_rename: dict, optional
        A dictionary specifying column renaming. Default is {}.
    :arg infer_schema: bool, optional
        Whether to infer the schema of the dataset. Default is True.
    :return: pyspark.sql.DataFrame
        Cleaned DataFrame
    """
    # Load data
    
    try:
        df = spark.read.csv(path_to_data, header=True, inferSchema=True)
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise IOError(f"Error loading data: {e}")
    
    # Drop specified columns
    df = df.drop(*columns_to_drop)
  
    # Rename columns
    for old_col, new_col in column_rename.items():
        df = df.withColumnRenamed(old_col, new_col)
   
    return df

def filter(df, filter_conditions={}):
    """
    function filters the DataFrame based on the specified conditions.

    :arg df: pyspark.sql.DataFrame
        The DataFrame to be filtered.
    :arg filter_conditions: dict, optional
        A dictionary specifying filtering conditions as column-value pairs.
        Default is an empty dictionary.

    :return: pyspark.sql.DataFrame
        Filtered DataFrame.
    """
    if filter_conditions:
        for column, values in filter_conditions.items():
            df =  df.filter(df[column].isin(values))

    return df

def join(df1, df2, join_key, join_type='inner'):
    """
    Function joins two DataFrames based on the specified join key and join type.

    :arg df1: Spark DataFrame
        The first DataFrame to join.
    :arg df2: Spark DataFrame
        The second DataFrame to join.
    :arg join_key: str
        The column name to join the DataFrames on.
    :arg join_type: str, optional
        The type of join to perform. Default is 'inner',  valid join types = ['inner', 'left', 'right', 'outer'].

    :return: Spark DataFrame
        Joined DataFrame after applying the join.
    """
    df_joined = df1.join(df2, on=join_key, how=join_type)
    return df_joined


def save(df, path_to_save: str):
    """
    Save the DataFrame to a CSV file.
    """
    # Specify the full path for saving the CSV file
    csv_path = path_to_save + "/joined_data.csv"

    # Write the DataFrame to CSV
    df.write.mode('overwrite').csv(csv_path, header=True)



def main(data_path_one: str, 
         data_path_two: str,
         data_path_out: str, 
         columns_to_drop1=[], 
         columns_to_drop2=[], 
         column_rename_1={}, 
         column_rename_2={},
         filter_conditions_1 = {},
         filter_conditions_2 = {},
         join_key='client_identifier', 
         join_type='inner') -> None:
    '''	
    Main function to load, clean, filter, join two data sets and save the result.

    :arg data_path_one: str
        File path to the first dataset.
    :arg data_path_two: str
        File path to the second dataset.
    :arg data_path_out: str
        File path to save the joined data.
    :arg columns_to_drop1: list, optional
        A list of column names to drop from the first dataset. Default is [].
    :arg columns_to_drop2: list, optional
        A list of column names to drop from the second dataset. Default is [].
    :arg column_rename_1: dict, optional
        A dictionary specifying column renaming for the first dataset. Default is {}.
    :arg column_rename_2: dict, optional
        A dictionary specifying column renaming for the second dataset. Default is {}.
    :arg filter_conditions_1: dict, optional
        A dictionary specifying filtering conditions for the first dataset. Default is {}.
    :arg filter_conditions_2: dict, optional
        A dictionary specifying filtering conditions for the second dataset. Default is {}.
    :arg join_key: str, optional
        The column name to join the datasets on. Default is 'client_identifier'.
    :arg join_type: str, optional
        The type of join to perform. Default is 'inner'.

    :return: None
    '''
    # Load and clean the data
    df1 = load_and_clean_data(data_path_one, 
                            columns_to_drop=columns_to_drop1, 
                            column_rename=column_rename_1)
    df2 = load_and_clean_data(data_path_two, 
                        columns_to_drop=columns_to_drop2, 
                        column_rename=column_rename_2)
    logging.info("Data loaded and cleaned")
    # filter the data in data_set one )
    df1 = filter(df1, filter_conditions_1)
    df2 = filter(df2, filter_conditions_2)
    logging.info("Data filtered")
    # Join the data
    df_joined = join(df1, df2, join_key, join_type)
    logging.info("Data joined")
    # Save the data
    save(df_joined, data_path_out)
    logging.info("Data saved")
    df_joined.show(5)

if __name__ == "__main__":
    main('dataset_one.csv', 'dataset_two.csv', 'client_data/joined_data',
         columns_to_drop1=['first_name', 'last_name'],
         columns_to_drop2=['cc_n'],
         column_rename_1={'id': 'client_identifier'},
         column_rename_2={'id': 'client_identifier', 'btc_a': 'bitcoin_address', 'cc_t': 'credit_card_type'},
         filter_conditions_1={'country': ['United Kingdom', 'Netherlands']},
         filter_conditions_2={}
         )