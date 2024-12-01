import sqlite3

# Path to the SQLite database file
db_path = r'E:\city_latencies.db'

# Connect to the SQLite database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("SELECT * FROM city_latencies")
rows = cursor.fetchall()
print("Rows in city_latencies:", rows[:100])  # Display first 100 rows if any


