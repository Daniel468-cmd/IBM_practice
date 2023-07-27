import mysql.connector
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("Erasmus.csv", header=True)

# selectare coloane
d2 = df.select(["Receiving Country Code", "Sending Country Code"])

# frecventa valori
d3 = d2.groupBy(["Receiving Country Code", "Sending Country Code"]).count()
d3 = df.groupBy(["Receiving Country Code", "Sending Country Code"]).count()
# sortare
d4 = d3.orderBy(["Receiving Country Code", "Sending Country Code"])
d4.show(n=df.count(), truncate=False)

# Configure your database connection details
db_config = {
    'host': 'localhost',        
    'user': 'root',       
    'password': 'Jbalvin288@',       
    'database': 'ibm_practice'    
}

#Try connecting to the database
try:
    connection = mysql.connector.connect(**db_config)
    print("Conectat la baza de date.")
except mysql.connector.Error as err:
    print(f"Eroare la conectarea la baza de date: {err}")

# Close the database connection
connection.close()
print("Conexiunea la baza de date a fost închisă.")
