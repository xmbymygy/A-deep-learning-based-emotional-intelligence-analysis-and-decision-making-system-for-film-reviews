CREATE DATABASE IF NOT EXISTS movie_reviews;
USE movie_reviews;

CREATE TABLE IF NOT EXISTS raw_reviews (
    id INT PRIMARY KEY AUTO_INCREMENT,
    movie_name VARCHAR(200) NOT NULL,
    comment TEXT NOT NULL,
    rating INT,
    comment_time DATETIME,
    user_name VARCHAR(100),
    crawl_time DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cleaned_reviews (
    id INT PRIMARY KEY AUTO_INCREMENT,
    comment TEXT NOT NULL,
    sentiment_label INT NOT NULL,
    raw_id INT UNIQUE,
    FOREIGN KEY (raw_id) REFERENCES raw_reviews(id)
);
