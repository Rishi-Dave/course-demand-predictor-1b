import sqlite3

# This creates (or connects to) a database file named 'class_space.db'
conn = sqlite3.connect('class_space.db')
cursor = conn.cursor()

# You can now execute SQL commands using the cursor
# Always remember to close the connection when done
conn.close()