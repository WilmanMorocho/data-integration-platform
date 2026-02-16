import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="data_platform",
    user="admin",
    password="admin"
)

print("Conectado OK")
