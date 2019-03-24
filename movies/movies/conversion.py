'''
CSCI 620 Project
TMDB Movie Dataset Analysis
Link to dataset: https://www.kaggle.com/tmdb/tmdb-movie-metadata/data

Dataset loading and JSON Flattening Idea inspired from:
Sophier Dane. 2017. TMDb Format Introduction. https://www.kaggle.com/sohier/tmdb-format-introduction,
(2017). [Online; accessed 08-October-2017].

Contributors:
Akshay Pudage (ap7558@rit.edu)
Pallavi Chandanshive (pvc8661@rit.edu)
Sahil Pethe (ssp5329@rit.edu)

This program converts the RAW dataset to SQL tables by flattening JSON tags and splitting the tables.
'''

import json
import pandas as pd
from sqlalchemy import create_engine

def load_file(filename,table):
    """
    This module loads the dataset in a Pandas dataframe.
    :param filename: path to file
    :param table: table name
    :return: dataframe object of the file
    """
    df = pd.read_csv(filename)
    if table == "movies":
        df['release_date'] = pd.to_datetime(df['release_date']).apply(lambda x: x.date())
        json_columns = ['genres', 'keywords', 'production_countries', 'production_companies', 'spoken_languages']
    else:
        json_columns = ['cast', 'crew']
    for column in json_columns:
        df[column] = df[column].apply(json.loads)
    return df

def flattenJSON(dataframe,column,table):
    """
    This module flattens the JSON tags in the dataframe.
    :param dataframe: dataframe object
    :param column: JSON column
    :param table: table name
    :return: flattened dataframe object for the column
    """
    if table == "movies":
        id = "id"
    else:
        id = 'movie_id'
    dataframe.apply(lambda row: [x.update({'movie_id': row[id]}) for x in row[column]], axis=1)
    df = []
    dataframe[column].apply(lambda x: df.extend(x))
    df = pd.DataFrame(df)
    return df

def writeToCSV(dataframe,filename, engine):
    """
    This module write the dataframe to a csv file.
    :param dataframe: dataframe object
    :param filename: path to csv file.
    :return:
    """
    index = ['cast','crew']
    index_status = False
    if filename not in index:
        index_status = True
    dataframe.to_sql(filename, engine)


def main():
    """
    This module loads the dataset, transforms JSON and splits the relations into csv files as required
    for the RDBMS.
    :return:
    """

    # Load the data into dataframes
    movies = load_file("tmdb_5000_movies.csv","movies")
    credits = load_file("tmdb_5000_credits.csv","credits")
    engine = create_engine('postgresql://postgres:1@localhost:5433/tmdb')

    # Flatten JSON tags
    credit_json = {}
    credit_json_columns = ['cast','crew']
    for column in credit_json_columns:
        credit_json[column] = flattenJSON(credits,column,'credits')

    movie_json = {}
    movie_json_columns = ['genres', 'keywords', 'production_countries', 'production_companies', 'spoken_languages']
    for column in movie_json_columns:
        movie_json[column] = flattenJSON(movies,column,'movies')

    person = {}
    for keys in credit_json:
        table = credit_json[keys]
        for tuple in table.itertuples():
            if tuple.id not in person:
                person[tuple.id] = tuple.name
        table.rename(columns={'id': 'person_id'},inplace = True)
        table.drop('name', axis=1,inplace = True)

        writeToCSV(credit_json[keys],keys, engine)

    person = pd.DataFrame(list(person.items()), columns=['person_id', 'name'])
    person = person.sort_values(['person_id'])
    person.to_sql('person', engine)

    production_company = {}

    for row,id,movie_id,name in movie_json['production_companies'].itertuples():
        if id not in production_company:
            production_company[id] = name

    movie_json['production_companies'] = movie_json['production_companies'].drop(movie_json['production_companies'].columns[[2]], axis=1)
    production_company = pd.DataFrame(list(production_company.items()), columns=['id','company_name'])
    production_company.to_sql('production_companies', engine)
    movie_json['production_companies'].columns.values[0] = "company_id"
    movie_json['production_companies'].columns.values[1] = "movie_id"
    movie_json['production_companies'].to_sql('production_movie', engine)

    # Split genres column into two tables.
    genres = {}

    for row,id,movie_id,genre in movie_json['genres'].itertuples():
        if id not in genres:
            genres[id] = genre


    movie_json['genres'] = movie_json['genres'].drop(movie_json['genres'].columns[[2]], axis=1)
    genres = pd.DataFrame(list(genres.items()), columns=['id','genre'])
    genres.to_sql('genres', engine)
    movie_json['genres'].columns.values[0] = "genre_id"
    movie_json['genres'].columns.values[1] = "movie_id"
    movie_json['genres'].to_sql('genre_movie', engine)

    # Split keywords column into two tables.
    keywords = {}

    for row,id,movie_id,keyword in movie_json['keywords'].itertuples():
        if id not in keywords:
            keywords[id] = keyword

    movie_json['keywords'] = movie_json['keywords'].drop(movie_json['keywords'].columns[[2]], axis=1)
    keywords = pd.DataFrame(list(keywords.items()), columns=['id','keyword'])
    keywords.to_sql('keywords', engine)
    movie_json['keywords'].columns.values[0] = "keyword_id"
    movie_json['keywords'].columns.values[1] = "movie_id"
    movie_json['keywords'].to_sql('keyword_movie', engine)

    # Split production_countries column into two tables.
    countries = {}
    i=1
    for row,iso,movie_id,country in movie_json['production_countries'].itertuples():
        if country not in countries:
            countries[country] = i
            i += 1
        movie_json['production_countries'].loc[row,'name'] = countries[country]

    countries = pd.DataFrame(list(countries.items()), columns=['country', 'id'])
    countries.to_sql('countries', engine)
    movie_json['production_countries'].columns.values[1] = "movie_id"
    movie_json['production_countries'].columns.values[2] = "country_id"
    movie_json['production_countries'].to_sql("country_movie", engine)

    # Split spoken_languages column into two tables.
    languages = {}
    i = 1
    for row, iso, movie_id, name in movie_json['spoken_languages'].itertuples():
        name = str(name)
        if '?' in name:
            name = "Unknown"
        if name not in languages:
            languages[name] = i
            i += 1
        movie_json['spoken_languages'].loc[row, 'name'] = languages[name]

    languages = pd.DataFrame(list(languages.items()), columns=['id', 'language'])
    languages.to_sql('languages', engine)
    movie_json['spoken_languages'].columns.values[1] = "movie_id"
    movie_json['spoken_languages'].columns.values[2] = "language_id"
    movie_json['spoken_languages'].to_sql("language_movie", engine)

    movies.drop(['genres', 'keywords', 'production_companies', 'production_countries', 'spoken_languages'], axis=1,inplace=True)
    movies.to_sql("movies", engine)

if __name__ == '__main__':
    main()