import subprocess
import time
import psycopg2
import pandas as pd

def wait_for_postgres(host, max_retries=5, delay_seconds=5):
    """Wait for PostgreSQL to become available."""
    retries = 0
    while retries < max_retries:
        try:
            conn = psycopg2.connect(dbname='movies_db', user='postgres', password='secret', host=host)
            conn.close()
            print("Successfully connected to PostgreSQL!")
            return True
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            retries += 1
            print(
                f"Retrying in {delay_seconds} seconds... (Attempt {retries}/{max_retries})")
            time.sleep(delay_seconds)
    print("Max retries reached. Exiting.")
    return False

# Use the function before running the ELT process
if not wait_for_postgres(host="movies_postgres"):
    exit(1)

print("Starting ELT script...")

# Configuration for the source PostgreSQL database
source_config = {
    'dbname': 'movies_db',
    'user': 'postgres',
    'password': 'secret',
    # Use the service name from docker-compose as the hostname
    'host': 'movies_postgres'
}

# Use psql to load the dumped SQL file into the destination database
load_command = [
    'psql',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],
    '-a', '-f', '/docker-entrypoint-initdb.d/insert_movies.sql'
]

# Set the PGPASSWORD environment variable to avoid password prompt
subprocess_env = dict(PGPASSWORD=source_config['password'])

# Execute the load command
subprocess.run(load_command, env=subprocess_env, check=True)

print("Ending ELT script...")