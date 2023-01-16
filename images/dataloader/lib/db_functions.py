# Function to write a dataframe to a mysql table
def write_table(dataframe, table_name, connection_info):
    url, user, password, driver = connection_info
    dataframe.write.format("jdbc").option("url", url)\
    .option("driver", driver)\
    .option("dbtable", table_name)\
    .option("user", user).option("password", password)\
    .mode("append")\
    .save()

# Function to query a mysql table
def query_table(query, connection_info, spark):
    url, user, password, driver = connection_info
    df = spark.read.format("jdbc").option("url", url)\
    .option("driver", driver)\
    .option("query", query)\
    .option("user", user).option("password", password)\
    .load()
    return df

# Function to truncate a mysql table
def truncate_table(table, cursor):
    cursor.execute(f"truncate table {table}")