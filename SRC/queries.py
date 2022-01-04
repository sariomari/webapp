import mysql.connector, requests




def correct(keywords):
    kw_string = """"""
    for i, keyword in enumerate(keywords):
        if i==len(keywords)-1:
            kw_string += "k.keyword = '{}'".format(keyword)
            break
        kw_string = kw_string + "k.keyword = '{}' OR ".format(keyword)
    return kw_string

## function that returns list of actors who acted in at least 4 categories and acted in a movie in the top 250

def versatile_actors():
    pass



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
lst = ['mafia', 'christmas']
print(correct(lst))
