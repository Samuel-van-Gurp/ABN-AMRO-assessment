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

def join_and_fillters (df1, df2, countries_of_intrest = ['United Kingdom','Netherlands']):
    """
    This function joins two DataFrames and filters rows based on specified countries of interest.

    :param df1: Spark DataFrame
        The first DataFrame to be joined containing customer information.
    :param df2: Spark DataFrame
        The second DataFrame to be joined containing financial information of costumer.
    :param countries_of_interest: list, optional
        A list of countries of interest to filter the data. Default is ['United Kingdom', 'Netherlands'].

    :return: Spark DataFrame
        Joined DataFrame after filtering rows based on the specified countries of interest.
    """
    # drop all rows with a country not in the countries_of_intrest list
    df1 = df1.filter(df1.country.isin(countries_of_intrest)) 
    # join the two datasets
    df_joined = df1.join(df2, 'client_identifier', 'inner')
    
    return df_joined

def save (df, path_to_save: str):
    """
    Save the joined dataset to a csv file.
    """
    df.write.csv(path_to_save, header=True) 


def main():
    # Load and clean the data
    df1, df2 = load_and_clean_data('dataset_one.csv', 'dataset_two.csv')
    # Join and fill the data
    df_joined = join_and_fillters()(df1, df2)
    # Save the data
    save(df_joined, 'joined_data.csv')
    df_joined.show(5)
    
if __name__=="__main__":
    main()