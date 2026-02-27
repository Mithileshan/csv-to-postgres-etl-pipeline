CREATE TABLE movies (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    genre VARCHAR(255),
    rating DECIMAL(3, 2),
    year INTEGER
);