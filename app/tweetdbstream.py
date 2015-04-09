#!/usr/bin/python

import tweetdb.tweetdb as tdb
import logging
import argparse
import sys
import time
from multiprocessing import cpu_count, Queue


def main():
    # command line option parsing stuff
    parser = argparse.ArgumentParser(description="Capture and store" +
                                     "streaming Twitter data.")
    danger_group = parser.add_argument_group("dangerous options",
                                             "Caution: use these options at "
                                             "your own risk. You may destroy "
                                             "your existing data.")
    danger_group.add_argument('-d', "--drop",
                              action="store_true", dest="dropflag",
                              default=False,
                              help="drop the all tables, erase all image data")
    danger_group.add_argument("-c", "--create",
                              action="store_true", dest="createflag",
                              default=False,
                              help="create a new set of tables")
    parser.add_argument("-v", "--verbose", default=False, action="store_true",
                        dest="verbose",
                        help="log to screen as well as logfile")

    parser.add_argument("parmfile", type=str, help='YAML parameter file')

    args = parser.parse_args()

    # parse YAML parmfile
    parmdata = tdb.read_parmdata(args.parmfile)

    # set up the logger
    logFormatter = logging.Formatter("%(asctime)s [%(filename)-5.5s] "
                                     "[%(levelname)-5.5s] [%(processName)-5s] "
                                     "%(message)s")
    rootLogger = logging.getLogger('__name__')
    rootLogger.setLevel('INFO')
    
    if parmdata['files']['log_file'] is not None:
        fileHandler = logging.FileHandler(parmdata['files']['log_file'],
                                          mode='w')
        fileHandler.setFormatter(logFormatter)
        rootLogger.addHandler(fileHandler)
 
    if args.verbose:
        consoleHandler = logging.StreamHandler(sys.stdout)
        consoleHandler.setFormatter(logFormatter)
        rootLogger.addHandler(consoleHandler)

    rootLogger.info('Starting.')

    # spin up my engine/authentication/queue
    rootLogger.info('Authenticating to Twitter.')
    auth = tdb.get_oauth(parmdata)
    rootLogger.info('Connecting to database.')
    engine = tdb.get_sql_engine(parmdata)
    rootLogger.info('Setting up data queue.')
    queue = Queue(100)
    
    # handle the drop/create table cases first
    if args.dropflag:
        tdb.drop_tables(engine)
        tdb.drop_images(parmdata)
        return

    if args.createflag:
        tdb.create_tables(engine)
  
    # spin up the tweet handlers
    if parmdata['settings']['num_consumers'] > cpu_count():
        rootLogger.info('Requested %d consumers. '
                        % parmdata['settings']['num_consumers'] +
                        'Limited to number of cores ' +
                        '(%d threads).' % cpu_count())
        parmdata['settings']['num_consumers'] = cpu_count()

    if parmdata['settings']['num_producers'] > 2:
        rootLogger.info('Requested %d producers. '
                        % parmdata['settings']['num_producers'] +
                        'Twitter API limits you to 2.')
        parmdata['settings']['num_producers'] = 2

    if parmdata['database']['db_type'].upper() == "SQLITE":
        rootLogger.info('Requested %d threads '
                        % parmdata['settings']['num_consumers'] +
                        'but SQLite supports only 1.')
        parmdata['settings']['num_consumers'] = 1
        parmdata['settings']['num_producers'] = 1

    consumers = []
    for i in range(parmdata['settings']['num_consumers']):
        consumers.append(tdb.tweet_consumer(queue, engine, parmdata,
                                            name="consumer_%d" % i))
        consumers[i].start()

    # begin streaming to database
    producers = []
    for i in range(parmdata['settings']['num_producers']):
        producers.append(tdb.tweet_producer(auth, queue, parmdata,
                                            name="producer_%d" % i))
        producers[i].start()
  
    #    while queue.qsize() > 0:
    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            rootLogger.info('Keyboard interrupt detected.  Depleting queue ' +
                            'and preparing to shutdown.')
            for producer in producers:
                producer.close()
            
            while queue.qsize() > 0:
                time.sleep(1)
            return

if __name__ == '__main__':
    main()
