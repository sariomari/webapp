import mysql.connector
import urllib
import requests

def main():
    conn = mysql.connector.connect(host="localhost", username="DbMysql48", password="DbMysql48", database="DbMysql48",
                                   port=3305)
    url = "https://imdb-api.com/en/API/Top250Movies/k_6tj9fsmd"
    payloads, headers = {}, {}
    res = requests.request("GET", url=url, data=payloads, headers=headers)
    print(res.text.encode("utf8"))
    print(conn)
    mdb = conn.cursor()
    mdb.execute("SELECT * FROM HW_Group")
    res = mdb.fetchall()
    for x in res:
        print(x)

    


main()