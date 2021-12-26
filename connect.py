from flask import Flask, render_template
import mysql.connector
import requests
import json

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('home.html')
@app.route('/about')
def about():
    return 'About Page'
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

    


if __name__ == '__main__':
    app.run(debug=True)
