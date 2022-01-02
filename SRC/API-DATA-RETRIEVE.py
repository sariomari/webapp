############### Python file which inserts data into the DataBase ###############
############### We used the public api tmdb to insert the table Top250Movies, and got the rest of the data from the following source: ###############
############### https://www.kaggle.com/rounakbanik/the-movies-dataset ###############
import csv
import mysql.connector, requests
from mysql.connector.pooling import CNX_POOL_NAMEREGEX
import ast

## i forgot to add column vote_count to movies, this function adds it - not for submission
def add_vote_count():
    file = open("/Users/sari/Downloads/movies_metadata.csv")
    mdb = connect_to_database()
    cursor = mdb.cursor()
    csvreader = csv.reader(file)
    next(csvreader)
    i=0
    for row in csvreader:
        if len(row) < 23:
            continue
        movie_id = row[5]
        vote_count = row[23]
        cursor.execute("""UPDATE movies SET vote_count = %s WHERE id = %s""", (vote_count, movie_id))
        i+=1
        if i%1000==0:
            print("Inserted {}".format(i))
            mdb.commit()
    mdb.commit()
    close_connection(mdb)

################################### QUERIES ###################################
############ a function that given two names (actor1, actor2), returns a list of movies that both actors acted in (if there's any), otherwise exits ############
def movies_with_two_actors(actor1, actor2):
    ## parsing actors first and last names (if they have a last name)
    a1 = actor1.split(" ")
    a2 = actor2.split(" ")
    a1_fn, a1_ln, a2_fn, a2_ln = None, None, None, None
    if len(a1) == 2:
        a1_ln = a1[1]
    a1_fn = a1[0]
    if len(a2) == 2:
        a2_ln = a2[1]
    a2_fn = a2[0]

    ## connecting to database
    mdb = connect_to_database()
    cursor = mdb.cursor()
    ## getting the ids of our actors - if not found in database then we handle the error
    id_query = """SELECT id FROM actors WHERE first_name = %s
                        AND last_name = %s"""
    cursor.execute(id_query, (a1_fn, a1_ln))
    res = cursor.fetchone()
    if not res:
        print("First actor not found in database, Please contact us.")
        return
    actor1_id = res[0]
    cursor.execute(id_query, (a2_fn, a2_ln))
    res = cursor.fetchone()
    if not res:
        print("Second actor not found in database, Please contact us.")
        return
    actor2_id = res[0]

    ## we now execute the main query and return the result if its non-empty
    movie_list = []
    main_query = """SELECT m.name, m.ranking 
                    FROM movies m, movie_actor ma1, movie_actor ma2
                    WHERE ma1.actor_id = {}
	                    AND ma2.actor_id = {}
	                    AND ma1.movie_id = m.id 
	                    AND ma2.movie_id = m.id
                    ORDER BY ranking DESC""".format(actor1_id, actor2_id)
    cursor.execute(main_query)
    res = cursor.fetchall()
    for row in res:
        movie_list.append(row)
    ## no movies found
    if len(movie_list) == 0:
        print("Actors {} and {} don't have any movies together in our database!".format(actor1, actor2))
        exit(-1)
    close_connection(mdb)
    return movie_list


############ a function that given (genre, n) returns actors who acted in more than n movies with specified genre, in descending order ############
############ simply explained - type in a genre and a number, and we'll give you the most experienced actors of this genre ############
def best_actors_in_genre(genre, n):
    res = []
    mdb = connect_to_database()
    cursor = mdb.cursor()
    cursor.execute("""  SELECT res.first_name, res.last_name, res.cnt AS movie_count
                        FROM (SELECT a.first_name, a.last_name, COUNT(DISTINCT m.name) AS cnt
	                          FROM actors a
	                          JOIN movie_actor ma ON a.id = ma.actor_id
	                          JOIN movies m ON m.id = ma.movie_id 
                              JOIN movie_genre mg ON mg.movie_id = ma.movie_id
                              JOIN genres g ON g.id = mg.genre_id 
                              WHERE g.genre = %s
                              GROUP BY a.first_name, a.last_name) res
                        WHERE res.cnt >= %s
                        ORDER BY movie_count DESC """, (genre, str(n)))
    rows = cursor.fetchall()
    for row in rows:
        res.append(row)
    return res

############ a function that returns a list of actors who have acted in at least n genres ############
def versatile_actors(n):
    res = []
    mdb = connect_to_database()
    cursor = mdb.cursor()
    cursor.execute("""  SELECT res.first_name, res.last_name, res.cnt AS genre_count
                        FROM (SELECT a.first_name, a.last_name, COUNT(DISTINCT g.id) AS cnt
	                          FROM actors a
                              JOIN movie_actor ma ON a.id = ma.actor_id
                              JOIN movies m ON ma.movie_id = m.id
                              JOIN movie_genre mg ON mg.movie_id = m.id
                              JOIN genres g ON g.id = mg.genre_id 
                              GROUP BY a.first_name, a.last_name) res
                        WHERE res.cnt >= 5
                        ORDER BY res.cnt DESC """)
    rows = cursor.fetchall()
    for row in rows:
        res.append(row)
    return res

############ a function that returns a list of all actors who have acted in at least 2 of the top 250 movies (imdb) - ordered by number of movies ############
def actors_in_top250movies():
    res = []
    mdb = connect_to_database()
    cursor = mdb.cursor()
    cursor.execute("""SELECT res.first_name, res.last_name, res.cnt AS numOfMovies
                      FROM (SELECT a.first_name, a.last_name, COUNT(DISTINCT tm.id) AS cnt
	                        FROM actors a
                            JOIN movie_actor ma ON ma.actor_id = a.id
                            JOIN top250movies tm ON tm.id = ma.movie_id
                            GROUP BY a.first_name, a.last_name) res
                      WHERE res.cnt >= 2
                      ORDER BY res.cnt DESC;""")
    rows = cursor.fetchall()
    for row in rows:
        res.append(row)
    return res

############ given a list of keywords, this function returns the best movies based on these keywords ############
def best_movies_with_keyword(keywords):
    kw_string = """"""
    for i, keyword in enumerate(keywords):
        if i==len(keywords)-1:
            kw_string += "k.keyword = '{}'".format(keyword)
            break
        kw_string = kw_string + "k.keyword = '{}' OR ".format(keyword)
    print(kw_string)
    res = []
    mdb = connect_to_database()
    cursor = mdb.cursor()
    cursor.execute("""SELECT m.name, m.ranking 
                      FROM movies m 
                      JOIN movie_keyword mk ON mk.movie_id = m.id 
                      JOIN keywords k ON k.id = mk.keyword_id 
                      WHERE {}
                      ORDER BY m.ranking DESC;""".format(kw_string))
    rows = cursor.fetchall()
    for row in rows:
        res.append(row)
    return res

############ given a director name, this function returns a list actors who have have been cast the most in the given director's movies ############
def director_favorite_actors(director_name):
    res = []
    fullname = director_name.split(" ", 1)
    director_fn = fullname[0]
    director_ln = ""
    if len(fullname) == 2:
        director_ln = fullname[1]
    mdb = connect_to_database()
    cursor = mdb.cursor()
    cursor.execute("""SELECT a.first_name, a.last_name, COUNT(*) as numOfMovies
                      FROM actors a
                      JOIN movie_actor ma ON a.id = ma.actor_id
                      JOIN movie_director md ON md.movie_id = ma.movie_id 
                      JOIN directors d ON d.id = md.director_id 
                      WHERE d.first_name = %s AND d.last_name = %s
                      GROUP BY a.first_name, a.last_name
                      ORDER BY numOfMovies DESC;""", (director_fn, director_ln))
    rows = cursor.fetchall()
    for row in rows:
        res.append(row)
    if len(res) == 0:
        print("No such director in our database.")
        return
    return res

############ as the name says, this function returns a list of the best movies for each year who have a vote count more than 1000 (so we filter unknown movies) ############
def best_movie_each_year():
    res = []
    mdb = connect_to_database()
    cursor = mdb.cursor()
    cursor.execute("""SELECT res.name, res.`year`
                      FROM (SELECT m.name, m.`year` , RANK() OVER (PARTITION BY m.`year` ORDER BY m.ranking DESC, m.vote_count) AS rk
                      FROM movies m 
                      WHERE m.vote_count >= 1000) res
                      WHERE res.rk = 1
                      ORDER BY res.`year` DESC;""")
    rows = cursor.fetchall()
    for row in rows:
        res.append(row)
    return res
########################------------MAIN PART-------------########################
######################## INSERTING DATA INTO THE DATABASE ########################

def main():
    insert_top250movies()
    insert_movies_genres()
    add_genres()
    insert_movie_actor()
    insert_directors()
    insert_movie_director()
    insert_keywords()
    change_primary_key("directors", "id")
    add_foreign_keys()



############ calling public api to insert top 250 movies to database ############
def insert_top250movies():
    ## connecting to database
    mdb = connect_to_database()
    cursor = mdb.cursor()
    ## arguments for GET request
    url = "https://api.themoviedb.org/3/list/634?api_key=c6017d35bc98fce7f99b8235c0d9241f&language=en-US"
    headers, payload = {}, {}
    ## response object
    res = requests.request("GET", url=url, data=payload, headers=headers)
    ## a json object which contains the movies
    top250movies = res.json()["items"]
    ## inserting all the movies
    for i in range(len(top250movies)):
        query = """INSERT INTO top250movies (id, ranking, title, `year`, rating) VALUES (%s, %s, %s, %s, %s)"""
        movie = top250movies[i]
        year = movie["release_date"].split("-", 1)[0]
        cursor.execute(query, (movie["id"], str(i+1), movie["title"], year, movie["vote_average"]))
        print("INSERTED MOVIE #{}".format(i+1))
    mdb.commit()
    close_connection(mdb)


## using credits.csv from k file to insert data into the following tables: actors, movie_actor, directors, movies_directors
def insert_movie_actor():
    file = open("/Users/sari/Downloads/credits.csv")
    mdb = connect_to_database()
    cursor = mdb.cursor()
    csvreader = csv.reader(file)
    ## the first row is the column names
    next(csvreader)
    i=0
    id = 1
    for row in csvreader:
        cast = ast.literal_eval(row[0])
        movie_id = row[2]
        movie_actor_query = """INSERT IGNORE INTO movie_actor VALUES (%s, %s)"""
        actor_query = """INSERT IGNORE INTO actors VALUES (%s, %s, %s)"""
        for entry in cast:
            actor_id = entry["id"]
            actor_name = entry["name"].split(" ")
            first_name = actor_name[0]
            last_name = None
            if len(actor_name) >= 2:
                last_name = ""
                for i in range(1,len(actor_name)):
                    last_name += actor_name[i]
            cursor.execute(movie_actor_query, (actor_id, movie_id))
            i+=1
            cursor.execute(actor_query, (actor_id, first_name, last_name))
            i+=1
        mdb.commit()
        if i%1000 == 0:
            print("Inserted {} entries".format(i))
    mdb.commit()
    close_connection(mdb)
    print("#################")

## function that inserts into directors table - because the director id's in the file are not unique, we have decided to make the tuple
## (first_name, last_name) as the primary key, then for each director we give an auto-increment id (which will also be unique)
def insert_directors():
    file = open("/Users/sari/Downloads/credits.csv")
    mdb = connect_to_database()
    cursor = mdb.cursor()
    csvreader = csv.reader(file)
    ## the first row is the column names
    next(csvreader)
    i=0
    id = 1
    for row in csvreader:
        crew = ast.literal_eval(row[1])
        for crew_member in crew:
            if crew_member["job"] != "Director":
                continue
            director_name = crew_member["name"].split(" ", 1)
            director_first_name = director_name[0]
            director_last_name = ""
            if len(director_name) == 2:
                director_last_name   = director_name[1]
            cursor.execute("""INSERT IGNORE INTO directors (first_name, last_name) VALUES (%s, %s)""", (director_first_name, director_last_name))
            i+=1
            mdb.commit()
            if i%1000 == 0:
                print("Inserted {} entries".format(i))
    mdb.commit()
    close_connection(mdb)

            

## function that inserts to the movie_director table - we use the director id (which we created) for a given director ##
## given first and last name, we fetch the id from directors table and use that id for the director ##
## later, we will make the id column in directors table the primary key ##
def insert_movie_director():
    file = open("/Users/sari/Downloads/credits.csv")
    mdb = connect_to_database()
    cursor = mdb.cursor()
    csvreader = csv.reader(file)
    ## the first row is the column names
    next(csvreader)
    i=0
    for row in csvreader:
        crew = ast.literal_eval(row[1])
        movie_id = row[2]
        for crew_member in crew:
            if crew_member["job"] != "Director":
                continue
            if i<1000:
                i+=1
                continue
            director_name = crew_member["name"].split(" ", 1)
            director_first_name = director_name[0]
            director_last_name = ""
            if len(director_name) == 2:
                director_last_name   = director_name[1]
            cursor.execute("""SELECT id FROM directors where first_name = %s AND last_name = %s""", (director_first_name, director_last_name))
            res = cursor.fetchone()
            director_id = res[0]
            cursor.execute("""INSERT IGNORE INTO movie_director VALUES (%s, %s)""", (director_id, movie_id))
            i+=1
            if i%1000 == 0:
                mdb.commit()
                print("Inserted {} entries".format(i))
    mdb.commit()
    print("DONE!")
    close_connection(mdb)
    

############ function that proccesses data from movies_metadata.csv and inserts suitable data intto tables: movie, movie_genre ############
def insert_movies_genres():
    file = open("/Users/sari/Downloads/movies_metadata.csv")
    mdb = connect_to_database()
    cursor = mdb.cursor()
    csvreader = csv.reader(file)
    next(csvreader)
    i=0
    for row in csvreader:
        genres = ast.literal_eval(row[3])
        movie_id = row[5]
        year = row[14].split("-")[0] if row[14] != "" else None
        title = row[20]
        rating = row[22] if row[22] != "" else None
        vote_count = row[23]
        for genre in genres:
            genre_id = genre["id"]
            cursor.execute("""INSERT IGNORE INTO movie_genre VALUES (%s, %s)""", (movie_id, genre_id))
            mdb.commit()
            i+=1
        cursor.execute("""INSERT IGNORE INTO movies VALUES (%s, %s, %s, %s, %s)""", (movie_id, title, year, rating, vote_count))
        mdb.commit()
        i+=1
        if i%500 == 0:
            print("Inserted {} entries".format(i))
    mdb.commit()
    print("DONE!")
    close_connection(mdb)

############ function that proccesses data from keywords.csv and inserts suitable data into tables keywords, movie_keyword ############
def insert_keywords():
    file = open("/Users/sari/Downloads/keywords.csv")
    mdb = connect_to_database()
    cursor = mdb.cursor()
    csvreader = csv.reader(file)
    next(csvreader)
    i=0
    for row in csvreader:
        movie_id = row[0]
        keywords = eval(row[1])
        for keyword in keywords:
            keyword_id = keyword["id"]
            word = keyword["name"]
            cursor.execute("""INSERT IGNORE INTO keywords VALUES (%s, %s)""", (keyword_id, word))
            cursor.execute("""INSERT IGNORE INTO movie_keyword VALUES (%s, %s)""", (movie_id, keyword_id))
            i+=2
            if i%1000 == 0:
                print("Inserted {} entries".format(i))
                mdb.commit()
    mdb.commit()
    close_connection(mdb)

############ a simple function that builds the genre table - we add only 20 genres ############
def add_genres():
    gset = set()
    file = open("/Users/sari/Downloads/movies_metadata.csv")
    mdb = connect_to_database()
    cursor = mdb.cursor()
    csvreader = csv.reader(file)
    next(csvreader)
    for row in csvreader:
        genres = eval(row[3])
        for genre in genres:
            genre_id = genre["id"]
            genre_name = genre["name"]
            gset.add(genre_name)
            cursor.execute("""INSERT IGNORE INTO genres VALUES (%s, %s)""", (genre_id, genre_name))
            mdb.commit()
            if len(gset) == 20:
                close_connection(mdb)
                return

############ a function that adds suitable foreign keys after we inserted the data to all the tables ############
def add_foreign_keys():
    mdb = connect_to_database()
    cursor = mdb.cursor()
    cursor.execute("""ALTER TABLE movie_actor ADD FOREIGN KEY (actor_id) REFERENCES actors(id),
                      ADD FOREIGN KEY (movie_id) REFERENCES movies(id);
                      """)
    cursor.execute("""ALTER TABLE movies_directors ADD FOREIGN KEY (director_id) REFERENCES directors(id),
                      ADD FOREIGN KEY (movie_id) REFERENCES movies(id);
                      """)
    cursor.execute("""ALTER TABLE movie_genre ADD FOREIGN KEY (movie_id) REFERENCES movies(id),
                      ADD FOREIGN KEY (genre_id) REFERENCES genres(id);
                      """)
    cursor.execute("""ALTER TABLE movie_keyword ADD FOREIGN KEY (movie_id) REFERENCES movies(id),
                      ADD FOREIGN KEY (keyword_id) REFERENCES keywords(id);""")
    cursor.execute("""ALTER TABLE top250movies 
                      ADD FOREIGN KEY (id) REFERENCES movies(id);""")
    mdb.commit()
    print("SUCCESS")
    close_connection()

############ a function that given arguments (table, new_key), makes new_key the new primary key for table ############
def change_primary_key(table, new_key):
    mdb = connect_to_database()
    cursor = mdb.cursor()
    ## dropping current primary key
    cursor.execute("""ALTER TABLE %s DROP PRIMARY KEY;""", (table))
    ## making new_key as the new primary_key - IT SHOULD BE UNIQUE!
    cursor.execute("""ALTER TABLE %s ADD PRIMARY KEY (%s)""", (new_key))
    mdb.commit()
    close_connection()
    return



############ helper function that returns how many genres we have (the result is 20) ############
def num_of_genres():
    genreset = set()
    file = open("/Users/sari/Downloads/movies_metadata.csv")
    csvreader = csv.reader(file)
    next(csvreader)
    ## init counter
    i=0
    for row in csvreader:
        genres = eval(row[3])
        for genre in genres:
            prev = len(genreset)
            genreset.add(genre["name"])
            ## if there's a new genre, reset counter
            if (len(genreset) == prev + 1):
                i=0
            ## if there's no change to genre set, increment counter
            elif len(genreset) == prev:
                i+=1
            ## if after 5000 times we still haven't found a new genre, it most probably means that we have found all genres
            if i==5000:
                break
    return len(genreset)



############--------------- CONNECTING TO DATABASE ---------------###############
############ function that connects to our MySQL database, returns the connection object ############
def connect_to_database():
    conn = mysql.connector.connect(host="localhost", username="DbMysql48", password="DbMysql48", database="DbMysql48", port=3305)
    if conn.is_connected():
        print("Successfully Connected")
        return conn
    print("Connection Failed")
    exit(-1)

############ function that closes the SQL connection for the given connection object ############
def close_connection(conn):
    conn.close()

print(best_movie_each_year())