from twitter.auth.authenicate import OAuthenicate
from tweepy.api import API


class TwitterAPI(object):
    def __init__(self):
        self.api = None
        self.auth = OAuthenicate()

    def get_api(self):
        self.api = API(self.auth.authenticate())
        return self.api