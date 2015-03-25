#!/usr/bin/python

import TweetDB

def main():
    FORMAT = "%(asctime)-15s %(message)s"
    logging.basicConfig(filename='TweetDB.log', level=logging.INFO, format=FORMAT)
    stream_to_db()

if __name__ == '__main__':
    main()
