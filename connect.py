import mysql.connector
import urllib

def main():
    conn = mysql.connector.connect(host="localhost", username="DbMysql48", password="DbMysql48", database="DbMysql48",
                                   port=3305)
    print(conn)
    mdb = conn.cursor()
    mdb.execute("SELECT * FROM HW_Group")
    res = mdb.fetchall()
    for x in res:
        print(x)


main()