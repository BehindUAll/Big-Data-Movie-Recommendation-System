'''
CSCI 620 Project
TMDB Movie Dataset Analysis
Link to dataset: https://www.kaggle.com/tmdb/tmdb-movie-metadata/data

Contributors:
Akshay Pudage (ap7558@rit.edu)
Pallavi Chandanshive (pvc8661@rit.edu)
Sahil Pethe (ssp5329@rit.edu)

This program is the main connection file for front end to back end. it links the front end request of the user to the
back end python code and retrieves the recommendation list which is then fetched by the front end.

'''


from django.shortcuts import render
import psycopg2 as p
import movies.analysis as analysis


# drop down movie list fetched from database
global movies
con = p.connect("dbname='tmdb' user = 'postgres' host = 'localhost' port = 5433 password='1'")
cur = con.cursor()
cur.execute("Select original_title from movies;")
previous_movies="Movie Title"
movies = cur.fetchall()
con.commit()


# request from front end processed
def index(request):

    listofmovies = []
    new_movies = []

    for movie in movies:
        for i in movie:
            new_movies.append(i)

    if request.method == 'POST':
        print(request.POST)
        value = request.POST.get('MOVIES', '')
        if value is not None:
            listofmovies = analysis.recommendMovie(value)
            listofmovies.pop(0)
            return render(request, 'index.html', {'moviedata': new_movies, 'data': listofmovies,'userquery':value})
    else:
        return render(request, 'index.html', {'moviedata': new_movies})



