from pyspark.sql import SparkSession
import sys,logging
from pyspark.sql.functions import lit
import py4j

# Logging configuration
formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
handler = logging.FileHandler('/logs/datasummarizer.log',mode='a')
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

# Function to query a mysql table
def query_table(query):
    df = spark.read.format("jdbc").option("url", url)\
    .option("driver", driver)\
    .option("query", query)\
    .option("user", user).option("password", password)\
    .load()
    return df

# Get the aggreggated data from mysql
query = """select country, count(*) as count from people
inner join cities on people.place_of_birth_id = cities.id 
inner join counties on cities.county_id = counties.id
group by country"""

try:
    logger.info("Connecting to the database...")
    summary = query_table(query)\
        .groupBy(lit(1))\
        .pivot("country")\
        .sum("count")\
        .drop("1")

    # write data to json
    # conversion to pandas to have a file instead of a folder with files
    summary.toPandas().to_json("/data/summary_output.json", orient="records")
    #summary.repartition(1).write.mode("overwrite").json("/data/summary_output.json")

    logger.info("Closing the application")

except py4j.protocol.Py4JJavaError as e:
    print('An error occurred while executing Spark code. See the log file for more info.')
    logger.exception(str(e))
except Exception as e:
    print("An error occurred while running the application. See the log file for more info.")
    logger.exception("An error occurred while running the application.")

