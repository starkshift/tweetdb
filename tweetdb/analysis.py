from __future__ import division
NAME = "tweetdb"
VERSION = "0.1"
DESCRIPTION = "Utilities for accessing tweets stored in a relational database."
AUTHOR = "Russell Miller"
AUTHOR_EMAIL = ""
URL = "https://github.com/starkshift/tweetdb"
LICENSE = "MIT"

import tweetdb as tdb
from tweetdb import User, Tweet, Hashtag, Geotag, Mention, URLData, Media
import sqlalchemy as sa
from datetime import datetime as dt
from datetime import timedelta


def getEarlierTime(hours=0, minutes=0, seconds=0):
    return dt.utcnow() - timedelta(hours=hours, minutes=minutes,
                                   seconds=seconds)

class DatabaseInterrogator(object):
    def getTweets(self, start, stop=None, lang='en', limit=None):
        if stop is None:
            stop = dt.utcnow()
            
        thisQuery = self.session.query(Tweet).\
                    filter(Tweet.date >= start).\
                    filter(Tweet.date <= stop).\
                    filter(sa.func.upper(Tweet.lang) == lang.upper())

        if limit is not None:
            return thisQuery.limit(limit).all()
        else:
            return thisQuery.all()

    def getGeotagLocations(self, start, stop=None, lang='en', limit=None):
        if stop is None:
            stop = dt.utcnow()

        thisQuery = self.session.query(Geotag.latitude,Geotag.longitude,Tweet.date).\
                    filter(Tweet.date >= start).\
                    filter(Tweet.date <= stop).\
                    filter(Geotag.tweetid == Tweet.tweetid).\
                    filter(sa.func.upper(Tweet.lang) == lang.upper())

        if limit is not None:
            return thisQuery.limit(limit).all()
        else:
            return thisQuery.all()

    def getPopularHashtags(self, start, stop=None, lang='en', limit=None):
        if stop is None:
            stop = dt.utcnow()

        thisQuery = self.session.query(Hashtag.tag,
                                       sa.func.count(Hashtag.tag).\
                                       label('total')).\
            group_by(Hashtag.tag).join(Tweet).\
            filter(Hashtag.tweetid == Tweet.tweetid).\
            filter(sa.func.upper(Tweet.lang) == lang.upper()).\
            filter(Tweet.date >= start).\
            filter(Tweet.date <= stop).\
            order_by('total desc')

        if limit is not None:
            return thisQuery.limit(limit).all()
        else:
            return thisQuery.all()

    def refresh_session(self):
        self.session = tdb.get_sql_session(self.parmdata)

    def __init__(self, parmfile, parmdata=None):
        if parmdata is None:
            self.parmdata = tdb.read_parmdata(parmfile)
        else:
            self.parmdata = parmdata
        self.session = tdb.get_sql_session(self.parmdata)
