import psycopg2

# Establish a connection to the PostgreSQL database
connection = psycopg2.connect(
    host="localhost",
    database="python-db",
    user="postgres",
    password="Root123$"
)

# Create a cursor object to interact with the database
cursor = connection.cursor()

# Define the SQL statement to create the power plan table
create_table_query = """
CREATE TABLE IF NOT EXISTS power_plans (
    id SERIAL PRIMARY KEY,
    plan_name VARCHAR(100) NOT NULL,
    price FLOAT NOT NULL,
    duration INT NOT NULL,
    provider VARCHAR(100) NOT NULL
);
"""

# Execute the SQL statement to create the table
cursor.execute(create_table_query)

# Define the SQL statement to insert data into the power plan table
insert_data_query = """
INSERT INTO power_plans (plan_name, price, duration, provider)
VALUES 
    ('Basic Plan', 50.0, 30, 'Provider A'),
    ('Premium Plan', 80.0, 60, 'Provider B'),
    ('Business Plan', 120.0, 90, 'Provider A'),
    ('Economy Plan', 40.0, 45, 'Provider B'),
    ('Corporate Plan', 150.0, 120, 'Provider C');
"""

# Execute the SQL statement to insert data into the table
cursor.execute(insert_data_query)

# Define the SQL statement to query data from the power plan table
query_data_query = """
SELECT provider, COUNT(*) as num_plans, AVG(price) as avg_price
FROM power_plans
GROUP BY provider
HAVING COUNT(*) >= 2 AND AVG(price) > 60.0;
"""

# Execute the SQL statement to query data from the table
cursor.execute(query_data_query)

# Fetch all the rows and print the results
rows = cursor.fetchall()
for row in rows:
    print("Provider:", row[0])
    print("Number of Plans:", row[1])
    print("Average Price:", row[2])
    print("---")

# Close the cursor and the connection

# Commit the changes and close the cursor
connection.commit()
cursor.close()
connection.close()