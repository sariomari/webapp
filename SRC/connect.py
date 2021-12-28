from flask import Flask, render_template
import mysql.connector, requests


# app = Flask(__name__, template_folder='templates')

# @app.route('/')
# def home():
#     return render_template('home.html')

# @app.route('/about')
# def about():
#     return render_template('about.html')


def main():
    url = "https://imdb-api.com/en/API/Top250Movies/k_6tj9fsmd"
    headers, payload = {}, {}
    res = requests.request("GET", url=url, data=payload, headers=headers)
    top250movies = res.json()["items"]
    cols = ["id", "rank", "title", "year", "imDbRating"]
    for i in range(5):
        vals = "INSERT INTO top250movies (id, rank, title, year, imdbRating) VALUES ("
        curr = top250movies[i]
        vals = concat_vals(vals, curr, cols)
        print(vals)

def concat_vals(vals, curr, cols):
    for col in cols:
        if col != "imDbRating":
            vals += "{}, ".format(curr[col])
        else:
            vals += "{})".format(curr[col])
    return vals

main()
    
