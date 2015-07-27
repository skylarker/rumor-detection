from time import clock
import time
import sys

from tweepy import Stream, StreamListener

from model.tweet import Tweet
from model.user import User
from twitter.auth.authenicate import OAuthenicate


start = clock()
print start


class TwitterStreamListener(StreamListener):
    def __init__(self):
        super(TwitterStreamListener, self).__init__()
        self.count = 0
        self.tweet = None
        self.user = None

    def on_status(self, status):
        try:
            if status.lang == 'en':  # collect only English tweets
                self.tweet = Tweet()
                self.user = User()

                self.tweet.text = status.text.encode('utf-8').replace('\n', '\\n')
                self.tweet.id = status.id
                self.tweet.created_at = status.created_at
                self.tweet.source = status.source
                self.tweet.possibly_sensitive = status.possibly_sensitive
                self.tweet.place = status.place

                self.user.screen_name = status.author.screen_name.encode('utf-8')
                self.user.id = status.author.id
                self.user.name = status.author.name.encode('utf-8')
                self.user.location = status.author.location
                self.user.time_zone = status.author.time_zone
                self.tweet.user = self.user

                if not ('RT @' in self.tweet.text):  # Exclude re-tweets
                    """
                    pprint(vars(self.user))
                    pprint(vars(self.tweet))
                    """
                    self.count += 1
                    print self.count

            time_pass = clock() - start
            if time_pass % 60 == 0:
                print "I have been working for", time_pass, "seconds."

        except Exception, e:
            print >> sys.stderr, 'Encountered Exception:', e
            pass

    def on_error(self, status_code):
        print 'Error: ' + repr(status_code)
        return True  # False to stop

    def on_delete(self, status_id, user_id):
        """
        Called when a delete notice arrives for a status

        """
        #print "Delete notice for %s. %s" % (status_id, user_id)
        return

    def on_limit(self, track):
        """
        Called when a limitation notice arrives

        """
        print "Limitation notice received: %s" % str(track)
        return

    def on_timeout(self):
        print >> sys.stderr, 'Timeout ...'
        time.sleep(10)
        return True


auth = OAuthenicate().authenticate()
streamTube = Stream(auth=auth, listener=TwitterStreamListener(),
                    timeout=300)
streamTube.sample()

timePass = time.clock() - start
print timePass