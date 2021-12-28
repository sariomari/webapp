############### Python file which inserts data into the DataBase ###############
import json
import mysql.connector, requests


def main():
    insert_top250movies()

## function that connects to the database, returns the connection object
def connect_to_databse():
    conn = mysql.connector.connect(host="localhost", username="DbMysql48", password="DbMysql48", database="DbMysql48", port=3305)
    if conn.is_connected():
        print("Successfully Connected")
        return conn
    print("Connection Failed")
    exit(-1)

## function that closes the connection
def close_connection(conn):
    conn.close()

## calling public api to insert top 250 movies to database
def insert_top250movies():
    mdb = connect_to_databse()
    cursor = mdb.cursor()
    url = "https://imdb-api.com/en/API/Top250Movies/k_6tj9fsmd"
    headers, payload = {}, {}
    res = requests.request("GET", url=url, data=payload, headers=headers)
    top250movies = res.json()["items"]
    cols = ["id", "rank", "title", "year", "imDbRating"]
    for i in range(len(top250movies)):
        vals = """INSERT INTO top250movies (id, ranking, title, `year`, imdbRating) VALUES ("""
        curr = top250movies[i]
        vals = concat_vals(vals, curr, cols)
        cursor.execute(vals)
        print("INSERTED MOVIE #{}".format(i+1))
    
        
    mdb.commit()
    close_connection(mdb)

def concat_vals(vals, curr, cols):
    for col in cols:
        if col == cols[-1]:
            vals += "{});".format(curr[col])
            return vals
        elif col == "id" or col == "title":
            vals += """ "{}", """.format(curr[col])
        else:
            vals += "{}, ".format(curr[col])


main()