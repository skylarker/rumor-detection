from twitter.api.get_api import TwitterAPI


class Crawler(object):
    def __init__(self):
        self.twitter_api = TwitterAPI()
        self.api = self.twitter_api.get_api()

    def play(self):
        for follower in self.api.followers():
            print follower.name, follower.location


if __name__ == '__main__':
    crawler = Crawler()
    crawler.play()