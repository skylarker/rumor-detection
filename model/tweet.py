class Tweet(object):
    def __init__(self):
        """
        ['contributors', 'truncated', 'text', 'in_reply_to_status_id', 'id', 'favorite_count', '_api', 'author',
         'retweeted', 'coordinates', 'timestamp_ms', 'entities', 'in_reply_to_screen_name', 'id_str', 'retweet_count',
          'in_reply_to_user_id', 'favorited', 'retweeted_status', 'source_url', 'user', 'geo', 'in_reply_to_user_id_str'
          , 'possibly_sensitive', 'lang', 'created_at', 'filter_level', 'in_reply_to_status_id_str', 'place', 'source']
        """
        self.id = None
        self.text = None
        self.user = None
        self.created_at = None
        self.source = None
        self.place = None
        self.possibly_sensitive = None