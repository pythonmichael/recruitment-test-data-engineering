from pyspark.sql import SparkSession
import unittest
import sys
import pymysql
import logging

sys.path.insert(1,'../')
import lib.db_functions as func


class TestDbFunctions(unittest.TestCase):

    global host, port, db, url, user, password, driver, connection_info, spark
    # mySQL connection info
    host = "localhost"
    port = 3306
    db = "codetest"
    url = f"jdbc:mysql://{host}:{port}/{db}"
    user = "codetest"
    password = "swordfish"
    driver = "com.mysql.cj.jdbc.Driver"
    connection_info = (url,user,password,driver)

    spark = SparkSession.builder.appName("data_loader").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    def test_query_table(self):

        # Create a customers table and insert data
        con = pymysql.connect(host = host, port = port, user = user, passwd = password, db = db)
        cursor = con.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS Customers(id int, LastName varchar(32), FirstName varchar(32))")
        cursor.execute("TRUNCATE TABLE Customers")
        cursor.execute("INSERT INTO Customers(id, LastName, FirstName) VALUES ('1', 'User', 'Test')")
        con.commit()

        # Query the customers table using the query_table function
        customers = func.query_table("select id, LastName, FirstName from Customers", connection_info, spark)

        customer_array = customers.collect()    


        # TEST 1
        self.assertEqual(len(customer_array), 1) 

        # TEST 2
        id = customer_array[0].id
        LastName = customer_array[0].LastName
        FirstName = customer_array[0].FirstName
        self.assertEqual((id,LastName,FirstName),(1,'User', 'Test'))

    def test_write_table(self):

        # Create a customers table and insert data
        con = pymysql.connect(host = host, port = port, user = user, passwd = password, db = db)
        cursor = con.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS Customers(id int, LastName varchar(32), FirstName varchar(32))")
        cursor.execute("TRUNCATE TABLE Customers")
        con.commit()

        # Create a dataframe with test data
        columns = ["id", "LastName", "FirstName"]
        data = [("1", "User", "Test2")]
        dfFromData = spark.createDataFrame(data).toDF(*columns)

        # Write the dataframe to the database using our function
        customers = func.write_table(dfFromData, "Customers", connection_info)

        # Retrieve the data from the table
        cursor.execute("SELECT * FROM Customers")
        rows = cursor.fetchall()

        # TEST 1
        self.assertEqual(len(rows), 1)
        
        # TEST 2
        id = rows[0][0]
        LastName = rows[0][1]
        FirstName = rows[0][2]
        self.assertEqual((id,LastName,FirstName),(1,'User', 'Test2'))


if __name__ == '__main__':
    unittest.main()