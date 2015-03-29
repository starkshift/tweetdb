from __future__ import division
NAME = "tweetdb"
VERSION = "0.1"
DESCRIPTION = """Utilities for storing tweets in an relational database."""
AUTHOR = "Russell Miller"
AUTHOR_EMAIL = ""
URL = "https://github.com/starkshift/tweetdb"
LICENSE = "MIT"

import tweepy
import pickle
import urllib3
import certifi
import zlib
import logging
import requests
import md5
import os
import threading
from yaml import load
from datetime import datetime as dt
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, DateTime, Integer, String, Boolean, BigInteger, \
    Float, Binary
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.exc import IntegrityError

# set up the sql base
Base = declarative_base()

# get rootLogger
log = logging.getLogger("__name__")


def read_parmdata(parmfile):
    # parse a YAML parameter file
    with open(parmfile, 'r') as f:
        return load(f)


def get_oauth(parmdata):
    # authenticate to twitter
    keys = pickle.load(open(parmdata['files']['twitter_keys'], 'rb'))
    auth = tweepy.OAuthHandler(keys['ConsumerKey'], keys['ConsumerSecret'])
    auth.set_access_token(keys['AccessToken'], keys['AccessTokenSecret'])
    return auth
    

def get_sql_engine(parmdata):
    if parmdata['database']['db_type'].upper() == 'SQLITE':
        return create_engine('sqlite:///' + parmdata['database']['db_host'],
                             echo=False)
    elif parmdata['database']['db_type'].upper() == 'POSTGRES':
        dblogin = pickle.load(open(parmdata['database']['db_login'], 'rb'))
        return create_engine('postgresql://' + dblogin['username'] + ':' +
                             dblogin['password'] + '@'
                             + parmdata['database']['db_host']
                             + '/' + parmdata['database']['db_name'],
                             echo=False)


def create_tables(engine):
    log.info('Creating database tables.')
    Base.metadata.create_all(engine)


def drop_tables(engine):
    dropflag = raw_input('WARNING: All tables in database will' +
                         'be dropped.  Proceed? [y/N] ')
    if dropflag.upper() == 'Y':
        log.info('Dropping database tables.')
        Base.metadata.drop_all(engine)


def drop_images(parmdata):
    image_path = parmdata['settings']['image_storage']['path']
    dropflag = raw_input('WARNING: Image storage directory (\'%s\') will be' +
                         'deleted.  Proceed? [y/N] '
                         % (image_path))
    if dropflag.upper() == 'Y':
        log.info('Remove image directory.')
        os.system('rm -fr "%s"' % image_path)


def read_timeline(engine, auth, parmdata, userid=None):
    api = tweepy.API(auth)
    
    try:
       # make session
       Session = sessionmaker(bind=engine)
       session = Session()

       # handle user info
       rawuser = api.get_user(userid)
       add_user(rawuser,session)

       # get tweets
       myCursor = tweepy.Cursor(api.user_timeline,id=userid)
       for rawtweet in myCursor.items():
           add_tweet(rawtweet,session,parmdata['settings']['get_images'])

       # commit
       session.commit()
    except:
       session.rollback()
       session.close()
       raise
  
    session.close()


def add_tweet(tweet,session,get_images=False,image_path=None,https=None):
   # check if we've already added this tweet
   if session.query(Tweet).filter(Tweet.tweetid == tweet.id).count() == 0:
      tweetobj = Tweet(tweet)
      session.add(tweetobj)
      
      for tag in tweet.entities['hashtags']:
          hashobj = Hashtag(tweet,tag)
          session.merge(hashobj)
          
      for mention in tweet.entities['user_mentions']:
          mentionobj = Mention(tweet,mention)
          session.merge(mentionobj)
          
      for url in tweet.entities['urls']:
          urlobj = URLData(tweet,url)
          session.merge(urlobj)
          
      if tweet.geo is not None:
          geotagobj = Geotag(tweet)
          session.merge(geotagobj)
                
      if (get_images) and ('media' in tweet.entities):
          for idx, media in enumerate(tweet.entities['media']):
              mediaobj = Media(tweet,media,idx,image_path,https)
              session.merge(mediaobj)

      session.commit()
      
   else:
      tweetobj = session.query(Tweet).filter(Tweet.tweetid==tweet.id).one()
      tweetobj.update(tweet)
      session.add(tweetobj)
      session.commit()


def add_user(user,session):
    if session.query(User).filter(User.userid==user.id).count() == 0:
        userobj = User(user)
        session.add(userobj)
        session.commit()
    else:
        userobj = session.query(User).filter(User.userid==user.id).one()
        userobj.update(user)
        session.add(userobj)
        session.commit()
          
class tweet_handler(threading.Thread):
    '''
    This class manages the queue of tweets waiting to be added to the SQL database
    '''

    # open https pool for grabbing url data
    https = urllib3.PoolManager(cert_reqs="CERT_REQUIRED", ca_certs=certifi.where())

    def __init__(self,queue,engine,parmdata,name=None):
        # initialize the thread
        threading.Thread.__init__(self,name=name)

        log.info("Starting new tweet handler.")
 
        # this is meant to be a daemon process, exiting when the program closes
        self.daemon = True

        # update the log from this thread at this interval
        self.log_interval = parmdata['settings']['log_interval']
 
        # bind this thread to the database
        log.info('Connecting to the database.')
        Session=sessionmaker(bind=engine)
        self.session = Session()

        # set the queue to pull tweets from
        self.queue = queue

        # twitter's stream filtering for languages is currently (March 2015) broken
        # (you need to have a search term in addition to a language in order to filter,
        # but we want the full stream so we'll do the language filter ourselves)
        self.languages  = parmdata['settings']['langs']

        for lang in self.languages:
            log.info('Logging tweets of language \'%s\'.'%lang)

        # set up whether we're getting tweeted images or not
        self.get_images = parmdata['settings']['get_images']
        log.info('Logging image file data is set to \'%s\'.'%self.get_images)

        if parmdata['settings']['image_storage']['method'].upper() == 'FILE':
            self.image_path = parmdata['settings']['image_storage']['path']
            if self.get_images:
                log.info('Image data being stored on filesystem at \'%s\''%self.image_path)
        
        # some diagnostic variables 
        self.last_time = dt.now()
        self.n_tweets = 0


    def run(self):
       while True:
          status = self.queue.get()
          self.queue.task_done()
          if any(status.lang in s for s in self.languages) or any('ALL' in s.upper() for s in self.languages):
              self.n_tweets+=1
              # there is a small chance that two threads will try to add the same user concurrently
              # this try statement serves to get arround the sqlalchemy IntegrityError which would result
              try:
                  add_user(status.author,self.session)
                  add_tweet(status,self.session,self.get_images,self.image_path,self.https)
              except IntegrityError:
                  self.session.rollback()
                  pass
              except KeyboardInterrupt:
                  pass
              except:
                  raise
          self.status_update()

    def status_update(self):
        '''
        Method for keeping track of the rate at which each tweet handler is processing the queue
        '''
        elapsed_time = (dt.now() - self.last_time).total_seconds()
        if elapsed_time > self.log_interval:
            log.info("Capturing %f tweets/second (%f sec elapsed time)."%(self.n_tweets/elapsed_time,elapsed_time))
            log.info("Reporting %d tweets remaining in queue."%self.queue.qsize())
            self.last_time = dt.now()
            self.n_tweets = 0



def stream_to_db(auth,engine,queue,parmdata):
    while True: 
       try:
          # set up twitter api
          api = tweepy.API(auth)

          # set up stream listener
          myListener = database_listener(api,queue)

          stream = tweepy.streaming.Stream(auth,myListener,timeout=60)
          stream.sample()
       except KeyboardInterrupt:
          log.info('Keyboard interrupt detected.  Depleting queue and preparing to shutdown.')
          stream.disconnect()
          log.info('Stream disconnected.')
          return
       except requests.packages.urllib3.exceptions.ProtocolError:
          continue
       except:
          while api.wait_on_rate_limit:
              time.sleep(10)
          queue.join()
          raise


###########################################################
#         Tweepy Listener Class Definitions
###########################################################
class database_listener(tweepy.StreamListener):
    '''
    Takes data received from the streaming API and places it in the
    queue to be processed by tweet_handlers  

    '''
   
    def on_status(self, status):
        self.queue.put(status)
        return True
 
    def on_error(self, status_code):
        log.info('Got an error with status code: ' + str(status_code))
        return True # To continue listening
 
    def on_timeout(self):
        log.info('Listener timeout.')
        return True # To continue listening

    def __init__(self,api,queue):
        self.api = api
        self.queue = queue
       


###########################################################
#            SQLAlchemy Class Definitions
###########################################################       
class Hashtag(Base): 
    """Hashtag Data"""
    __tablename__ = "Hashtag"
    tweetid = Column('tweetid',BigInteger,ForeignKey("Tweet.tweetid"),unique=False,primary_key=True)
    tag = Column('tag',String,unique=False)
        
    def __init__(self,tweet,tag):
        self.tweetid = tweet.id
        self.tag = tag['text']

class Media(Base): 
    """Binary Media Data"""
    __tablename__ = "Media"
    tweetid = Column('tweetid',BigInteger,ForeignKey("Tweet.tweetid"),unique=False,primary_key=True)
    blob = Column('blob',Binary,unique=False,nullable=True)
    native_filename = Column('native_filename',String,unique=False,nullable=True)
    local_filename = Column('local_filename',String,unique=False,nullable=True)
    
    def __init__(self,tweet,media,idx,image_path=None,https=None):
        self.tweetid = tweet.id
        rawdata = https.request('GET',media['media_url_https']).data
        extension = os.path.splitext(media['media_url_https'])[1]
        self.native_filename = os.path.split(media['media_url_https'])[1]
        if image_path is None:
            self.blob = zlib.compress(rawdata)
            self.filename = None
        else:
            self.blob = None
            md5hash = md5.new()
            md5hash.update(str(tweet.id) + str(idx))
            hashdata = md5hash.hexdigest()
            self.local_filename = image_path + os.path.sep + hashdata[0:2] + os.path.sep + hashdata[3:5] + os.path.sep + hashdata[6:8] + os.path.sep + hashdata[9:] + extension
            if not os.path.exists(os.path.dirname(self.local_filename)):
                os.makedirs(os.path.dirname(self.local_filename),mode=0777)
            with open(self.local_filename,'wb') as f:
                f.write(rawdata)
            

class URLData(Base): 
    """URL Data"""
    __tablename__ = "URLData"
    tweetid = Column('tweetid',BigInteger,ForeignKey("Tweet.tweetid"),unique=False,primary_key=True)
    url = Column('url',String,unique=False)
    
    def __init__(self,tweet,url):
        self.tweetid = tweet.id
        self.url = url['expanded_url']

class Mention(Base):
    """User Mention Data"""
    __tablename__ = "Mention"
    tweetid = Column('tweetid',BigInteger,ForeignKey("Tweet.tweetid"),unique=False,primary_key=True)
    source = Column('source',BigInteger,unique=False)
    target = Column('target',BigInteger,unique=False)
    
    def __init__(self,tweet,mention):
        self.tweetid = tweet.id
        self.source = tweet.author.id
        self.target = mention['id']
 

class Geotag(Base):
    """Geotag Data"""
    __tablename__ = "Geotag"
    tweetid = Column('tweetid',BigInteger,ForeignKey("Tweet.tweetid"),unique=False,primary_key=True)
    latitude = Column('latitude',Float,unique=False)
    longitude = Column('longitude',Float,unique=False)
    
    def __init__(self,tweet):
        self.tweetid = tweet.id
        self.latitude = tweet.geo['coordinates'][0] 
        self.longitude = tweet.geo['coordinates'][1]  

class Tweet(Base):
    """Tweet Data"""
    __tablename__ = "Tweet"

    tweetid = Column(BigInteger,primary_key=True)
    userid = Column('userid',BigInteger,ForeignKey("User.userid"))
    text = Column('text',String(length=500),nullable=True)
    rtcount = Column('rtcount',Integer)
    fvcount = Column('fvcount',Integer)
    lang = Column('lang',String)
    date = Column('date',DateTime)
    source = Column('source',String)
    geotags = relationship(Geotag,primaryjoin=tweetid==Geotag.tweetid,lazy="dynamic")
    hashtags = relationship(Hashtag,primaryjoin=tweetid==Hashtag.tweetid,lazy="dynamic")
    mentions = relationship(Mention,primaryjoin=tweetid==Mention.tweetid,lazy="dynamic")
    urls = relationship(URLData,primaryjoin=tweetid==URLData.tweetid,lazy="dynamic")
    media = relationship(Media,primaryjoin=tweetid==Media.tweetid,lazy="dynamic")

    def __init__(self,tweet):
        self.tweetid = tweet.id
        self.userid = tweet.author.id
        self.text = tweet.text
        self.rtcount = tweet.retweet_count
        self.fvcount = tweet.favorite_count
        self.lang = tweet.lang
        self.date = tweet.created_at
        self.source = tweet.source

    def update(self,tweet):
        self.rtcount = tweet.retweet_count
        self.fvcount = tweet.favorite_count
  

class User(Base):
    """Twitter User Data"""
    __tablename__ = "User"

    userid = Column(BigInteger,primary_key=True)
    username = Column('username',String)
    name = Column('name',String)
    location = Column('location',String,nullable=True)
    description = Column('description',String,nullable=True)
    numfollowers = Column('numfollowers',Integer)
    numfriends = Column('numfriends',Integer)
    numtweets = Column('numtweets',Integer) 
    createdat = Column('createdat',DateTime)
    timezone = Column('timezone',String)
    geoloc = Column('geoloc',Boolean)
    lastupdate = Column('lastupdate',DateTime)
    verified = Column('verified',Boolean)
    tweets = relationship(Tweet,primaryjoin=userid==Tweet.userid,lazy="dynamic")

    def __init__(self,author):  
        self.userid = author.id
        self.username = author.screen_name
        self.name = author.name
        self.location = author.location
        self.description = author.description
        self.numfollowers = author.followers_count
        self.numfriends = author.friends_count
        self.numtweets = author.statuses_count
        self.createdat = author.created_at
        self.timezone = author.time_zone
        self.geoloc = author.geo_enabled
        self.verified = author.verified
        self.lastupdate = dt.now()

    def update(self,author):  
        self.username = author.screen_name
        self.name = author.name
        self.location = author.location
        self.description = author.description
        self.numfollowers = author.followers_count
        self.numfriends = author.friends_count
        self.numtweets = author.statuses_count
        self.timezone = author.time_zone
        self.geoloc = author.geo_enabled
        self.verified = author.verified
        self.lastupdate = dt.now()
