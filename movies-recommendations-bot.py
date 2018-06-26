import tweepy
import pandas as pd
import os

print('Running in Heroku!')

CSV_URL = os.environ.get('CSV_URL', None)
CONSUMER_KEY = os.environ.get('CONSUMER_KEY', None)
CONSUMER_SECRET = os.environ.get('CONSUMER_SECRET', None)
ACCESS_TOKEN = os.environ.get('ACCESS_TOKEN', None)
ACCESS_TOKEN_SECRET = os.environ.get('ACCESS_TOKEN_SECRET', None)
IMDB_URL = os.environ.get('IMDB_URL', None)

def init_dataset():
    dataset = pd.read_csv(CSV_URL)
    dataset.set_index('imdb_id', inplace=True)
    return dataset

def get_movie_recommendation(username):
    recommendation = dataset.sample(1)
    for row in recommendation.itertuples():
        tweet = f'Hola, {username}! Te recomiendo: \n'
        tweet += f'{row.title} ({row.year}) [{row.spanish_title}] \n'
        tweet += f'{row.genres} \n' 
        tweet += f'Puntuación: {row.rating} ({row.votes} votos) \n'
        tweet += f'{IMDB_URL}{row.Index}'
        return tweet

def process_status(status):
    # Verify that the status isn't a reply nor a quote.
    if status.in_reply_to_status_id or status.is_quote_status:
        return

    username = f'@{status.author.screen_name}'
    tweet_id = status.id
    recommendation = get_movie_recommendation(username)

    api.update_status(recommendation, tweet_id)

    #hashtags = [x['text'] for x in status.entities['hashtags']]
    #print('Hashtags:', hashtags)    
    #print('Name:', status.author.name)
    #print('Text:', status.text)

def init_tweepy_api():
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)
    return api

class MentionsStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        process_status(status)

dataset = init_dataset()
api = init_tweepy_api()

mentionsStreamListener = MentionsStreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=mentionsStreamListener)
myStream.filter(track=['@MoviesMasterBot'])