import tweepy
import pandas as pd
import os

CSV_URL = os.environ.get('CSV_URL', None)
CONSUMER_KEY = os.environ.get('CONSUMER_KEY', None)
CONSUMER_SECRET = os.environ.get('CONSUMER_SECRET', None)
ACCESS_TOKEN = os.environ.get('ACCESS_TOKEN', None)
ACCESS_TOKEN_SECRET = os.environ.get('ACCESS_TOKEN_SECRET', None)
IMDB_URL = os.environ.get('IMDB_URL', None)
BOT_SCREEN_NAME = os.environ.get('BOT_SCREEN_NAME', None)
BOT_USERNAME = f'@{BOT_SCREEN_NAME}'

GENRES = {
    'accion': 'Acción',
    'acción': 'Acción',
    'Accion': 'Acción',
    'Acción': 'Acción',
    'aventura': 'Aventura',
    'Aventura': 'Aventura',
    'Animación': 'Animación',
    'Animacion': 'Animación',
    'animación': 'Animación',
    'animacion': 'Animación',
    'Comedia': 'Comedia',
    'comedia': 'Comedia',
    'Crimen': 'Crimen',
    'crimen': 'Crimen',
    'Documental': 'Documental',
    'documental': 'Documental',
    'Drama': 'Drama',
    'drama': 'Drama',
    'Familia': 'Familia',
    'familia': 'Familia',
    'Fantasía': 'Fantasía',
    'fantasía': 'Fantasía',
    'Fantasia': 'Fantasía',
    'fantasia': 'Fantasía',
    'Historia': 'Historia',
    'historia': 'Historia',
    'Terror': 'Terror',
    'terror': 'Terror',
    'Música': 'Música',
    'Musica': 'Música',
    'música': 'Música',
    'musica': 'Música',
    'Misterio': 'Misterio',
    'misterio': 'Misterio',
    'Romance': 'Romance',
    'romance': 'Romance',
    'Scifi': 'Ciencia ficción',
    'scifi': 'Ciencia ficción',
    'CienciaFicción': 'Ciencia ficción',
    'cienciaficción': 'Ciencia ficción',
    'CienciaFiccion': 'Ciencia ficción',
    'cienciaficcion': 'Ciencia ficción',
    'Suspense': 'Suspense',
    'suspense': 'Suspense',
    'Guerra': 'Guerra',
    'guerra': 'Guerra',
    'Western': 'Western',
    'western': 'Western',
}

def init_dataset():
    dataset = pd.read_csv(CSV_URL)
    dataset.set_index('imdb_id', inplace=True)
    return dataset

def get_movie_recommendation(username, hashtags):
    recommendation = dataset[dataset.genres.str.contains('|'.join(hashtags))].sample(1)
    for row in recommendation.itertuples():
        tweet = f'Hola, {username}! Te recomiendo: \n'
        tweet += f'{row.title} ({row.year}) [{row.spanish_title}] \n'
        tweet += f'{row.genres} \n' 
        tweet += f'Puntuación: {row.rating} ({row.votes} votos) \n'
        tweet += f'{IMDB_URL}{row.Index}'
        return tweet

def process_status(status):
    # Verify that the status isn't a quote.
    if status.is_quote_status:
        return

    # If the status is a reply to a recommendation, reply with another one. If not, ignore.
    if status.in_reply_to_status_id and status.in_reply_to_screen_name != BOT_SCREEN_NAME:
        return

    # If the status is a reply to a recommendation, but the authors differ, ignore the tweet.
    if status.in_reply_to_status_id and status.in_reply_to_screen_name == BOT_SCREEN_NAME:
        previous_tweet = api.get_status(status.in_reply_to_status_id_str)
        if previous_tweet.in_reply_to_screen_name != status.author.screen_name:
            return

    # Retrieve hashtags from the tweet. Discard any that doesn't exist in the genre list.
    hashtag_list = [GENRES[x['text']] for x in status.entities['hashtags'] if x['text'] in GENRES.keys()]

    username = f'@{status.author.screen_name}'
    tweet_id = status.id
    recommendation = get_movie_recommendation(username, hashtag_list)

    api.update_status(recommendation, tweet_id)

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
myStream.filter(track=[BOT_USERNAME])