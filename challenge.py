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

def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
    df[kaggle_column] = df.apply(
        lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
        , axis=1)
    df.drop(columns=wiki_column, inplace=True)

def movieRatingETL(wikiData_raw, kaggleData_raw, ratingData_raw):
    print("Starting ETL process")
    print("Starting Extract process")

    file_dir = 'the-movies-dataset/'
    with open(f'{wikiData_raw}', mode='r') as file:
        wikiData = json.load(file)

    #wiki_movies_df = pd.DataFrame(wikiData)
    kaggleData = pd.read_csv(f'{file_dir}{kaggleData_raw}', low_memory=False)
    ratingData = pd.read_csv(f'{file_dir}{ratingData_raw}', low_memory=False)

    print("Start Wiki Data Process")
    ############
    # Wiki Data
    ############
    wiki_movies = [movie for movie in wikiData
        if ('Director' in movie or 'Directed by' in movie) 
            and 'imdb_link' in movie
            and 'No. of episodes' not in movie]
    
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
    wiki_movies_df = pd.DataFrame(clean_movies)

    #REGEX Duplicate Drop - imdb link
    try:
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
    except:
        print("No Duplicates")
    
    #Remove mostly null columns
    try:
        wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
        wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
    except:
        print("No Mostly Null columns")

    # REGEX box office data
    # Example for form 1 --> $123.4 (m/b)illion
    form_one = r"\$\s*\d+\.?\d*\s*[mb]illi?on"

    # Example for form 2 --> $123,456,789
    form_two = r"\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)"


    #Prepare to Parse Data
    try: 
        box_office = wiki_movies_df['Box office'].dropna()
    except:
        box_office = wiki_movies_df['Box office']
        print("No Box office rows to drop due to N/A")
    try:
        box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)  #Convert lists to strings
        box_office = box_office.str.replace(r'\$.*[---](?![a-z])', '$', regex=True)     #Remove values between dollar sign and hyphan
        box_office = box_office.str.replace(r'\[\d+\]\s*', '')  # Remove citation reference
    except:
        print("Box Office Reg Ex fail")

    
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_movies_df.drop('Box office', axis=1, inplace=True)
    

    #Parse Budget Data
    try:
        budget = wiki_movies_df['Budget'].dropna()
    except:
        budget = wiki_movies_df['Budget']
        print("No Budget rows to drop due to N/A")
    try:
        budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
        budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
        budget = budget.str.replace(r'\[\d+\]\s*', '')
    except:
        print("Budget Reg Ex fail")

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

    try: 
        release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)
        wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract \
            (f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    except:
        print("Date Extraction failure")

    #Parse Running Time
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

    try:
        #hours & minutes or minutes
        running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')

        #covert string to numeric values, empty str to NaN
        running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)

        #Convert hours to minutes
        wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)

    except:
        print("Extraction of running time failed")

    try:    
        wiki_movies_df.drop('Running time', axis=1, inplace=True)
    except:
        print("Running time column drop failed")

    ############
    # Kagle Data
    ############
    print("Start Kagle Data Process")
    # Clean Kagle Data

    #Gets non-adult movies & drops column
    kaggleData = kaggleData[kaggleData['adult'] == 'False'].drop('adult',axis='columns') 

    kaggleData['video'] = kaggleData['video'] == 'True'
    kaggleData['budget'] = kaggleData['budget'].astype(int)
    kaggleData['id'] = pd.to_numeric(kaggleData['id'], errors='raise')
    kaggleData['popularity'] = pd.to_numeric(kaggleData['popularity'], errors='raise')
    kaggleData['release_date'] = pd.to_datetime(kaggleData['release_date'])

    ############
    # Merge Data
    ############
    print("Merge Wiki & Kagle")
    movies_df = pd.merge(wiki_movies_df, kaggleData, on='imdb_id', suffixes=['_wiki','_kaggle'])
    
    try: 
        # Drop extreme date mismatches 
        movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & \
            (movies_df['release_date_kaggle'] < '1965-01-01')].index)
        movies_df = movies_df.drop(movies_df[(movies_df['release_date_kaggle'] > '1996-01-01') & \
            (movies_df['release_date_wiki'] < '1965-01-01')].index)
    except: 
        print("extreme date mismatch problem")

    # Competing data:
    # Wiki                     Movielens                Resolution
    #--------------------------------------------------------------------------
    # title_wiki               title_kaggle             Drop Wiki
    # running_time             runtime                  Keep Kaggle; fill in zeros with Wikipedia
    # budget_wiki              budget_kaggle            Keep Kaggle; fill in zeros with Wikipedia
    # box_office               revenue                  Keep Kaggle; fill in zeros with Wikipedia
    # release_date_wiki        release_date_kaggle      Drop Wiki
    # Language                 original_language        Drop Wiki
    # Production company(s)    production_companies     Drop Wiki
    try:
        movies_df.drop(columns=['title_wiki', 'release_date_wiki', 'Language', 'Production company(s)'], inplace=True)
    except:
        print("Could not drop columns")

    try:
        fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
        fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
        fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
    except:
        print("Could not fill data")

    try:
    #Reorder & Rename columns
        movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                        'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                        'genres','original_language','overview','spoken_languages','Country',
                        'production_companies','production_countries','Distributor',
                        'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                        ]]

        movies_df.rename({'id':'kaggle_id',
                        'title_kaggle':'title',
                        'url':'wikipedia_url',
                        'budget_kaggle':'budget',
                        'release_date_kaggle':'release_date',
                        'Country':'country',
                        'Distributor':'distributor',
                        'Producer(s)':'producers',
                        'Director':'director',
                        'Starring':'starring',
                        'Cinematography':'cinematography',
                        'Editor(s)':'editors',
                        'Writer(s)':'writers',
                        'Composer(s)':'composers',
                        'Based on':'based_on'
                        }, axis='columns', inplace=True)
    except:
        print("Could not re-arrange/rename files")
    
    ########################
    # Ratings Transformation
    ########################
    print("Start Ratings Process")
    # Raname userID to count
    rating_counts = ratingData.groupby(['movieId','rating'], as_index=False).count() \
                    .rename({'userId':'count'}, axis=1) \
                    .pivot(index='movieId',columns='rating', values='count')
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns] # Append "rating_" to each column. Example: rating_3.5


    #MERGE
    print("Merge Ratings into movie")
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)

    
    ###############
    # Load into SQL
    ###############
    try:
        ### SQL Connection
        db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
        engine = create_engine(db_string)

        movies_df.to_sql(name='movies', con=engine, if_exists='append')

        ### Loading status update
        rows_imported = 0
        start_time = time.time()
        for data in ratingData, chunksize=1000000):

            # print out the range of rows that are being imported
            print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')

            data.to_sql(name='ratings', con=engine, if_exists='append')
            rows_imported += len(data)

            # print that the rows have finished importing
            # add elapsed time to final print out
            print(f'Done. {time.time() - start_time} total seconds elapsed')
    except:
        print("Could not load data into SQL")

    #END
    completeString = "Movie ETL complete"
    return completeString


#ETL Main Project Flow
# From other function that Extracts Data
# Import files/data (EXTRACT) 
wiki_raw  = 'wikipedia-movies.json'
kaggle_raw = 'movies_metadata.csv'
rating_raw = 'ratings.csv'

print(movieRatingETL(wiki_raw, kaggle_raw, rating_raw))