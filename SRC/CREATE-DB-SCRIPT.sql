CREATE TABLE IF NOT EXISTS actors (
	id INT(11),
	first_name VARCHAR(100) NOT NULL,
	last_name VARCHAR(100),
	gender CHAR(1),
	PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS movies (
	id INT(11) NOT NULL,
	name VARCHAR(100) NOT NULL,
	year INT(11) NOT NULL,
	ranking FLOAT,
	PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS directors (
	id INT(11) NOT NULL,
	first_name VARCHAR(100) NOT NULL,
	last_name VARCHAR(100),
	PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS movie_actor (
	actor_id INT(11) NOT NULL,
	movie_id INT(11) NOT NULL,
	PRIMARY KEY (actor_id, movie_id)
);



CREATE TABLE IF NOT EXISTS movies_directors (
	director_id INT(11) NOT NULL,
	movie_id INT(11) NOT NULL,
	PRIMARY KEY (director_id, movie_id)
);



CREATE TABLE IF NOT EXISTS movie_genre (
	movie_id INT(11) NOT NULL,
	genre VARCHAR(100) NOT NULL,
	PRIMARY KEY (movie_id, genre)
);


CREATE TABLE IF NOT EXISTS directors_genres (
	director_id int(11),
	genre VARCHAR(100),
	prob FLOAT,
	PRIMARY KEY (director_id, genre)
);

CREATE TABLE IF NOT EXISTS keywords (
	id INT(11),
	keyword VARCHAR(100),
	PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS movie_keyword (
	movie_id INT(11),
	keyword_id INT(11),
	PRIMARY KEY (movie_id, keyword_id)
);

CREATE TABLE IF NOT EXISTS top250movies (
	imdb_id INT(11) NOT NULL,
	ranking INT(11),
	title VARCHAR(100) NOT NULL,
	year INT(11) NOT NULL,
	imdbRating INT(11),
	PRIMARY KEY (ranking)
);
