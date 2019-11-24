# Import Dependencies
import json
import pandas as pd
import numpy as np
import re #regular expression
import time
from sqlalchemy import create_engine #SQL Import/export
import psycopg2
from config import db_password

def clean_movie(movie):
    movie = dict(movie)
    alt_titles = {}
    # combine alternate titles into one list
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune–Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
        if key in movie:
            alt_titles[key] = movie[key]
            movie.pop(key)
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles
    # merge column names
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)

        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')

        return movie

def parse_dollars(s):
    # if s is not a string, return NaN
    if type(s) != str:
        return np.nan

    # if input is of the form $###.# million
    if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
        s = re.sub('\$|\s|[a-zA-Z]','', s)  # remove dollar sign and " million"       
        value = float(s) * 10**6            # convert to float and multiply by a million
        return value                        # return value

    # if input is of the form $###.# billion
    elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
        s = re.sub('\$|\s|[a-zA-Z]','', s)  # remove dollar sign and " billion"
        value = float(s) * 10**9            # convert to float and multiply by a billion
        return value                        # return value

    # if input is of the form $###,###,###
    elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
        s = re.sub('\$|,','', s)    # remove dollar sign and commas
        value = float(s)            # convert to float
        return value                # return value

    # otherwise, return NaN
    else:
        return np.nan


def movieRaitingETL(wikiData, kaggleData, ratingData):
    # Import files/data (EXTRACT) --> Shift to outside of function
    raw_file  = 'wikipedia-movies.json'
    with open(f'{raw_file}', mode='r') as file:
        wiki_movies_raw = json.load(file)

    file_dir = 'the-movies-dataset/'
    kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv')
    ratings = pd.read_csv(f'{file_dir}ratings.csv')

    #Transform (Wiki Movies)
    wiki_movies = [movie for movie in wiki_movies_raw
        if ('Director' in movie or 'Directed by' in movie) 
            and 'imdb_link' in movie
            and 'No. of episodes' not in movie]
    wiki_movies_df = pd.DataFrame(wiki_movies)
    
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
    wiki_movies_df = pd.DataFrame(clean_movies)

    #REGEX Duplicate Drop - imdb link
    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
    wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
    
    #Remove mostly null columns
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns \
        if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
    

    # REGEX box office data
    # Example for form 1 --> $123.4 (m/b)illion
    form_one = r"\$\s*\d+\.?\d*\s*[mb]illi?on"

    # Example for form 2 --> $123,456,789
    form_two = r"\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)"


    #Prepare to Parse Data
    box_office = wiki_movies_df['Box office'].dropna()
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)  #Convert lists to strings
    box_office = box_office.str.replace(r'\$.*[---](?![a-z])', '$', regex=True)     #Remove values between dollar sign and hyphan
    box_office = box_office.str.replace(r'\[\d+\]\s*', '')  # Remove citation reference
    
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_movies_df.drop('Box office', axis=1, inplace=True)
    

    #Parse Budget Data
    budget = wiki_movies_df['Budget'].dropna()
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    budget = budget.str.replace(r'\[\d+\]\s*', '')

    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_movies_df.drop('Budget', axis=1, inplace=True)


    #Release Date
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

    #DF1 - Full month name, one- to two-digit day, four-digit year (i.e., January 1, 2000)
    #DF2 - Four-digit year, two-digit month, two-digit day, with any separator (i.e., 2000-01-01)
    #DF3 - Full month name, four-digit year (i.e., January 2000)
    #DF4 - Four-digit year
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}' 

    release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract \
        (f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)


    #Parse Running Time
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

    #hours & minutes or minutes
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')

    #covert string to numeric values, empty str to NaN
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)

    #Convert hours to minutes
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)

    wiki_movies_df.drop('Running time', axis=1, inplace=True)



    ############
    # Kagle Data
    ############

    #Gets non-adult movies & drops column
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns') 







    #END
    return something