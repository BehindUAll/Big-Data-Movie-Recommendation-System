'''
CSCI 620 Project
TMDB Movie Dataset Analysis
Link to dataset: https://www.kaggle.com/tmdb/tmdb-movie-metadata/data

Contributors:
Akshay Pudage (ap7558@rit.edu)
Pallavi Chandanshive (pvc8661@rit.edu)
Sahil Pethe (ssp5329@rit.edu)

This program creates a new database for the TMDB dataset.
'''
import psycopg2 as p

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def main():
    """
    This program creates a database for the TMDB dataset
    :return:
    """
    con = p.connect("user = 'postgres' host = 'localhost' port = 5433 password='1'")
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    cur.execute("CREATE DATABASE tmdb")
    con.commit()

    con1 = p.connect("dbname = 'tmdb' user = 'postgres' host = 'localhost' port = 5433 password='1'")
    cur1 = con1.cursor()
    con1.commit()

if __name__ == '__main__':
    main()


