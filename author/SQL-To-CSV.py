import pandas as pd
import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="sinta"
)

df = pd.read_sql("SELECT * FROM authors", conn)
df.to_csv("data.csv", index=False)
