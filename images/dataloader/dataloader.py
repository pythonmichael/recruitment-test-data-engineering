from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_date
import sys,logging
import pymysql
import py4j

# Logging configuration
formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
handler = logging.FileHandler('/logs/dataloader.log',mode='a')
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# Creating spark session
spark = SparkSession.builder.appName("data_loader").master("local").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
logger.info("Starting spark application")

# mySQL connection info
host = "database"
port = 3306
db = "codetest"
url = f"jdbc:mysql://{host}:{port}/{db}"
user = "codetest"
password = "swordfish"
driver = "com.mysql.cj.jdbc.Driver"

# Function to write a dataframe to a mysql table
def write_table(dataframe, table_name):
    dataframe.write.format("jdbc").option("url", url)\
    .option("driver", driver)\
    .option("dbtable", table_name)\
    .option("user", user).option("password", password)\
    .mode("append")\
    .save()

# Function to query a mysql table
def query_table(query):
    df = spark.read.format("jdbc").option("url", url)\
    .option("driver", driver)\
    .option("query", query)\
    .option("user", user).option("password", password)\
    .load()
    return df

# Function to truncate a mysql table
def truncate_table(table):
    cursor.execute(f"truncate table {table}")

try:
    # JDBC connection used for table truncation
    logger.info("Connecting to the database...")
    con = pymysql.connect(host = host, port = port, user = user, passwd = password, db = db)
    cursor = con.cursor()
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    truncate_table("people")
    truncate_table("cities")
    truncate_table("counties")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    con.close()

    # Read the source files
    places_raw = spark.read.option("header","true").csv("/data/places.csv")
    people_raw = spark.read.option("header","true").csv("/data/people.csv")\
        .withColumn("date_of_birth", to_date(col("date_of_birth"), "yyyy-MM-dd"))

    # Create the counties dataframe and write it to the mysql table
    counties = places_raw.select("county","country").distinct()\
        .withColumnRenamed("county","name")

    write_table(counties, "counties")

    # Read the mysql counties table to retrieve the primary keys
    counties_tbl = query_table("select id, name as county, country from counties")

    # Create the counties dataframe and write it to the mysql table
    cities = places_raw.select("city","county","country")\
        .join(counties_tbl, ["county","country"])\
        .select(places_raw.city, counties_tbl.id)\
        .withColumnRenamed("city","name")\
        .withColumnRenamed("id","county_id")

    write_table(cities, "cities")

    cities_tbl = query_table("select id, name as city from cities")

    people = people_raw.join(cities_tbl, people_raw.place_of_birth == cities_tbl.city)\
        .drop("place_of_birth", "city")\
        .withColumnRenamed("id","place_of_birth_id")

    write_table(people, "people")

    logger.info("Closing the application")

except pymysql.err.MySQLError as e:
    print('Error connecting to database: {!r}, errno is {}. See the log file for more info.'.format(e, e.args[0]))
    logger.exception('Error connecting to database: {!r}, errno is {}'.format(e, e.args[0]))
except py4j.protocol.Py4JJavaError as e:
    print('An error occurred while executing Spark code. See the log file for more info.')
    logger.exception(str(e))
except Exception as e:
    print("An error occurred while running the application. See the log file for more info.")
    logger.exception("An error occurred while running the application.")