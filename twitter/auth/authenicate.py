import json

from tweepy import OAuthHandler


class OAuthenicate(object):
    def __init__(self):
        pass

    @staticmethod
    def authenticate():
        credentials_file_path = '/Users/arunprasathshankar/Desktop/rumor_detection/oauth.json'
        credentials = json.loads(open(credentials_file_path, 'r+').read())
        auth = OAuthHandler(credentials['consumer_key'], credentials['consumer_secret'])
        auth.set_access_token(credentials['access_token'], credentials['access_token_secret'])
        return auth
