############### Python file which inserts data into the DataBase ###############
############### We used a public api to insert the table Top250Movies, and the rest of the data we got from the following source: ###############
############### https://www.kaggle.com/rounakbanik/the-movies-dataset ###############
import csv
from os import close
import mysql.connector, requests
import ast


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
    mdb = connect_to_databse()
    cursor = mdb.cursor()
    ## getting the ids of our actors - if not found in database then we handle the error
    id_query = """SELECT id FROM actors WHERE first_name = %s
                        AND last_name = %s"""
    cursor.execute(id_query, (a1_fn, a1_ln))
    res = cursor.fetchone()
    if len(res) == 0:
        print("First actor not found in database, Please contact us.")
        exit(-1)
    actor1_id = res[0]
    cursor.execute(id_query, (a2_fn, a2_ln))
    res = cursor.fetchone()
    if len(res) == 0:
        print("Second actor not found in database, Please contact us")
        exit(-1)
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


def main():
    # insert_movie_actor_director()
    # insert_movies_genres()
    # insert_keywords()
    add_foreign_keys()



############ function that connects to our MySQL database, returns the connection object ############
def connect_to_databse():
    conn = mysql.connector.connect(host="localhost", username="DbMysql48", password="DbMysql48", database="DbMysql48", port=3305)
    if conn.is_connected():
        print("Successfully Connected")
        return conn
    print("Connection Failed")
    exit(-1)

############ function that closes the SQL connection for the given connection object ############
def close_connection(conn):
    conn.close()

############ calling public api to insert top 250 movies to database ############
def insert_top250movies():
    ## connecting to database
    mdb = connect_to_databse()
    cursor = mdb.cursor()
    ## arguments for GET request
    url = "https://imdb-api.com/en/API/Top250Movies/k_6tj9fsmd"
    headers, payload = {}, {}

    res = requests.request("GET", url=url, data=payload, headers=headers)
    ## a json object which contains the movies
    top250movies = res.json()["items"]
    ## inserting all the movies
    for i in range(len(top250movies)):
        query = """INSERT INTO top250movies (imdb_id, ranking, title, `year`, imdbRating) VALUES (%s, %s, %s, %s, %s)"""
        movie = top250movies[i]
        cursor.execute(query, (movie["id"][2:], movie["rank"], movie["title"], movie["year"], movie["imDbRating"]))
        print("INSERTED MOVIE #{}".format(i+1))
    mdb.commit()
    close_connection(mdb)


## using credits.csv from k file to insert data into the following tables: actors, movie_actor, directors, movies_directors
def insert_movie_actor():
    file = open("/Users/sari/Downloads/credits.csv")
    mdb = connect_to_databse()
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
    mdb = connect_to_databse()
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

def add_last_names():
    file = open("/Users/sari/Downloads/credits.csv")
    mdb = connect_to_databse()
    cursor = mdb.cursor()
    csvreader = csv.reader(file)
    ## the first row is the column names
    next(csvreader)
    i=0
    id = 1
    for row in csvreader:
        crew = ast.literal_eval(row[1])
        for crew_member in crew:
            if i==988:
                print("DONE!")
                mdb.commit()
                close_connection(mdb)
                return
            if crew_member["job"] != "Director":
                continue
            if len(crew_member["name"].split(" ")) <= 2:
                continue
            director_name = crew_member["name"].split(" ", 1)
            cursor.execute("""UPDATE directors SET last_name = %s WHERE first_name = %s AND last_name = "" """, (director_name[1], director_name[0]))
            i+=1

            

## function that inserts to the movie_director table - we use the director id (which we created) for a given director ##
## given first and last name, we fetch the id from directors table and use that id for the director ##
## later, we will make the id column in directors table the primary key ##
def insert_movie_director():
    file = open("/Users/sari/Downloads/credits.csv")
    mdb = connect_to_databse()
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
            director_name = crew_member["name"].split(" ")
            director_first_name = director_name[0]
            director_last_name = ""
            if len(director_name) == 2:
                director_last_name   = director_name[1]
            cursor.execute("""SELECT id FROM directors where first_name = %s AND last_name = %s""", (director_first_name, director_last_name))
            res = cursor.fetchone()
            # if (res == None):
            #     print(director_first_name, director_last_name)
            #     close_connection(mdb)
            #     return
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
    file = open("/Users/sari/Desktop/mmd.csv")
    mdb = connect_to_databse()
    cursor = mdb.cursor()
    csvreader = csv.reader(file)
    next(csvreader)
    i=0
    for row in csvreader:
        if len(row) < 23:
            continue
        genres = ast.literal_eval(row[3])
        movie_id = row[5]
        year = row[14].split("-")[0] if row[14] != "" else None
        title = row[20]
        rating = row[22] if row[22] != "" else None
        for genre in genres:
            genre_id = genre["id"]
            cursor.execute("""INSERT IGNORE INTO movie_genre VALUES (%s, %s)""", (movie_id, genre_id))
            mdb.commit()
            i+=1
        cursor.execute("""INSERT IGNORE INTO movies VALUES (%s, %s, %s, %s)""", (movie_id, title, year, rating))
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
    mdb = connect_to_databse()
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
    mdb = connect_to_databse()
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
    mdb = connect_to_databse()
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
    mdb.commit()
    print("SUCCESS")
    close_connection()




############ function that returns how many genres we have (the result is 20) ############
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


add_last_names()
