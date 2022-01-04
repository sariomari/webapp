CREATE TABLE IF NOT EXISTS actors (
	id INT(11),
	first_name VARCHAR(100) NOT NULL,
	last_name VARCHAR(100),
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
	id INT(11) NOT NULL AUTO_INCREMENT,
	first_name VARCHAR(60) NOT NULL,
	last_name VARCHAR(60),
	PRIMARY KEY (first_name, last_name),
	UNIQUE(id)
);

CREATE TABLE IF NOT EXISTS movie_actor (
	actor_id INT(11) NOT NULL,
	movie_id INT(11) NOT NULL,
	PRIMARY KEY (actor_id, movie_id)
);



CREATE TABLE IF NOT EXISTS movie_director (
	director_id INT(11) NOT NULL,
	movie_id INT(11) NOT NULL,
	PRIMARY KEY (director_id, movie_id)
);

CREATE TABLE IF NOT EXISTS genres (
	id INT(11),
	genre VARCHAR(20),
	PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS movie_genre (
	movie_id INT(11) NOT NULL,
	genre_id INT(11) NOT NULL,
	PRIMARY KEY (movie_id, genre),
);



CREATE TABLE IF NOT EXISTS directors_genres (
	director_id int(11),
	genre VARCHAR(100),
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
	id INT(11) NOT NULL,
	ranking INT(11),
	title VARCHAR(100) NOT NULL,
	year INT(11) NOT NULL,
	rating INT(11),
	PRIMARY KEY (ranking)
);