from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("costumer_data").getOrCreate()

def load_and_clean_data(path_to_data1: str, path_to_data2: str):
    """
    function loads the two CSV files into Spark DataFrames.
    then it cleans the data by removing personal identifiable information from the first dataset, excluding emails.
    and by removing credit card number from the second dataset.
    and renaming the columns for clarity.

    :param path_to_data1: str
        The file path to the first dataset.
    :param path_to_data2: str
        The file path to the second dataset.

    :return: Tuple containing two Spark DataFrames
        Cleaned DataFrame from the first dataset and cleaned DataFrame from the second dataset.    
    """
    # Load dataset_one and dataset_two
    df1 = spark.read.csv(path_to_data1, header=True, inferSchema=True)
    df2 = spark.read.csv(path_to_data2, header=True, inferSchema=True)

    # clean dataset_one, by removing  personal identifiable information from the first dataset, excluding emails.
    df1 = df1.drop('first_name', 'last_name')
    # rename id to client_identifier
    df1 = df1.withColumnRenamed('id', 'client_identifier')

    # Clean dataset_two, by removing credit card number.
    df2 = df2.drop('client_identifier')

    # rename columns 
    df2 = df2.withColumnRenamed('id', 'client_identifier')
    df2 = df2.withColumnRenamed('btc_a','bitcoin_address')
    df2 = df2.withColumnRenamed('cc_t',	'credit_card_type')

    return df1, df2
