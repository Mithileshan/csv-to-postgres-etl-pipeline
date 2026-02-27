FROM postgres:16.2

# Set environment variables
ENV POSTGRES_DB=movies_db
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=secret

# Copy the SQL script to the docker entrypoint directory so it's executed on startup
COPY insert_movies.sql /docker-entrypoint-initdb.d/