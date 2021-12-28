CREATE TABLE IF NOT EXISTS actors (
	id int(11),
	first_name varchar(100) NOT NULL,
	last_name varchar(100),
	gender char(1),
	PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS movies (
	id int(11) NOT NULL,
	name VARCHAR(100) NOT NULL,
	year INT(11) NOT NULL,
	ranking FLOAT,
	PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS directors (
	id INT(11) NOT NULL,
	first_name VARCHAR(11) NOT NULL,
	last_name VARCHAR(11),
	PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS movie_actor (
	actor_id INT(11) NOT NULL,
	movie_id INT(11) NOT NULL,
	PRIMARY KEY (actor_id),
	PRIMARY KEY (movie_id)
);

CREATE TABLE IF NOT EXISTS movies_directors (
	director_id INT(11) NOT NULL,
	movie_id INT(11) NOT NULL,
	PRIMARY KEY (director_id),
	PRIMARY KEY (movie_id)
);

CREATE TABLE IF NOT EXISTS movie_genre (
	movie_id INT(11),
	genre VARCHAR(100),
	PRIMARY KEY (movie_id, genre)
);

