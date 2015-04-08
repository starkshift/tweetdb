#!/usr/bin/python

from flask import Flask
from flask import render_template, request
from tweetdb import analysis as tdba
from tweetdb import tweetdb as tdb
import json
from bson import json_util
from bson.json_util import dumps
from datetime import datetime
from datetime import timedelta
import pprint

app = Flask(__name__)

mydb = tdba.DatabaseInterrogator('/data/config/stream.conf')


@app.route('/')
def index():
    minutes = 5
    pprint.pprint(mydb.getPopularHashtags(tdba.getEarlierTime(minutes=minutes)))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
