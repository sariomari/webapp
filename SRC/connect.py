from flask import Flask, render_template
import mysql.connector


app = Flask(__name__, template_folder='templates')

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/about')
def about():
    return render_template('about.html')


def connect_to_databse():
    conn = mysql.connector.connect(host="localhost", username="DbMysql48", password="DbMysql48", database="DbMysql48", port=3305)
    if conn.is_connected():
        print("Successfully Connected")
        return conn
    print("Connection Failed")
    exit(-1)


def close_connection(conn):
    conn.close()
    
    




if __name__ == '__main__':
    app.run(debug=True)
