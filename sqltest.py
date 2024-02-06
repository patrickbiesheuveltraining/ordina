import pandas as pd
import pyodbc

conn = pyodbc.connect("Driver={SQL Server};Server=pbsql123.database.windows.net;UID=pbadmin;PWD=Pa$$w0rd;Database=Northwind;")
df = pd.read_sql_query('select * from SalesLT.Customer', conn)
print(df.head())
#print(df.to_string())
