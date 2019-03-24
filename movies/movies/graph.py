'''
CSCI 620 Project
TMDB Movie Dataset Analysis
Link to dataset: https://www.kaggle.com/tmdb/tmdb-movie-metadata/data

Contributors:
Akshay Pudage (ap7558@rit.edu)
Pallavi Chandanshive (pvc8661@rit.edu)
Sahil Pethe (ssp5329@rit.edu)

This program plots the graphs for popular keywords & genres.
'''

import pandas as pd
import movies.analysis as analysis
import matplotlib.pyplot as plt
import sqlalchemy as p

def plotPopularGenre(movies,connectionSQL):
    """
    Plots a graph of popular Genres in the dataframe
    :param movies: movies dataframe
    :return:
    """
    genres = pd.read_sql_table('genres',connectionSQL)
    genre_movie = pd.read_sql_table('genre_movie',connectionSQL)
    movies = analysis.addGenres(movies, genres, genre_movie)
    genre = {}
    for i,row in movies.iterrows():
        if not pd.isnull(row.genre):
            genres = row.genre.split(",")
            for g in genres:
                if g not in genre:
                    genre[g] = 1
                else:
                    genre[g] += 1
    
    genre = [[key,value] for key,value in genre.items()]
    genre.sort(key=lambda x: x[1], reverse=True)
    
    
    plt.figure(figsize=(25,10), dpi=125)
    plt.xlabel('Genre')
    plt.ylabel('Occurrence')
    plt.suptitle("Popular Genres")
    plt.bar(range(len(genre)), [list[1] for list in genre], align='center')
    plt.xticks(range(len(genre)), [list[0] for list in genre])
    plt.savefig('popularGenre.png')
    plt.show()


    
def plotPopularKeywords(movies,connectionSQL):
    """
    Plots a graph of top 30 keywords in the datafrae
    :param movies:movies dataframe
    :return:
    """
    keywords = pd.read_sql_table('keywords',connectionSQL)
    keyword_movie = pd.read_sql_table('keyword_movie',connectionSQL)
    movies = analysis.addKeywords(movies,keywords,keyword_movie)
    keyword = {}
    for i, row in movies.iterrows():
        if not pd.isnull(row.keyword):
            genres = row.keyword.split(",")
            for g in genres:
                if g not in keyword:
                    keyword[g] = 1
                else:
                    keyword[g] += 1

    keyword = [[key, value] for key, value in keyword.items()]
    keyword.sort(key=lambda x: x[1], reverse=True)
    keyword = keyword[0:30]
    plt.figure(figsize=(25, 20), dpi=125)
    plt.xlabel('Keyword')
    plt.ylabel('Occurrence')
    plt.suptitle("Top 30 Keywords")
    plt.bar(range(len(keyword)), [list[1] for list in keyword], align='center')
    plt.xticks(range(len(keyword)), [list[0] for list in keyword],rotation=90)
    plt.savefig('popularKeywords.png')
    plt.show()


def main():
    connectionSQL = p.create_engine('postgresql://postgres:1@localhost:5433/tmdb')
    movies = pd.read_sql_table('movies',connectionSQL)
    plotPopularKeywords(movies,connectionSQL)
    plotPopularGenre(movies,connectionSQL)

if __name__ == '__main__':
    main()