#!/usr/bin/python

import TweetDB.src.TweetDB as TDB
import logging
import argparse


def main():
    # command line option parsing stuff
    parser = argparse.ArgumentParser(description="Capture and store streaming Twitter data.")
    danger_group = parser.add_argument_group("Dangerous Options",
                    "Caution: use these options at your own risk.  "
                    "You may destroy your existing data.")
    danger_group.add_argument('-d',"--drop",
                  action="store_true", dest="dropflag", default=False,
                  help="drop the TweetDB tables")
    danger_group.add_argument("-c", "--create",
                  action="store_true", dest="createflag",default=False,
                  help="create a new set of tables")
    parser.add_argument("-v", "--verbose",default=True,action="store_true",
                      help="log to screen as well as logfile")

    parser.add_argument("parmfile",type=str,help = 'YAML parameter file')

    args = parser.parse_args()

    # parse YAML parmfile
    parmdata = TDB.read_parmdata(args.parmfile)

    # spin up my engine/authentication 
    auth = TDB.get_oauth(parmdata)
    engine = TDB.get_sqlite_engine(parmdata)

    # handle the drop/create table cases first
    if args.dropflag:
        TDB.drop_tables(engine)
        return

    if args.createflag:
        TDB.create_tables(engine)
        return

    # set up the logger
    #FORMAT = "%(asctime)-15s %(message)s"
    #logging.basicConfig(filename='TweetDB.log', level=logging.INFO, format=FORMAT)
    
    # begin streaming to database
    TDB.stream_to_db(auth=auth,engine=engine,parmdata=parmdata)

if __name__ == '__main__':
    main()
