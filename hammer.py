import time

import os
import sys
import getopt

import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode

PATTERN_CONSISTENT = 1  # consistent
PATTERN_SPIKEY     = 2  # spikey

# from config import DB_USER, DB_PASS and DB_NAME
DB_USER     = os.environ.get("DB_USER", None)
DB_PASS     = os.environ.get("DB_PASS", None)
DB_NAME     = os.environ.get("DB_NAME", None)
WORK_TYPE   = os.environ.get("WORK_TYPE", None)
OP_RATE     = os.environ.get("OP_RATE", None)
OP_TIME     = os.environ.get("OP_TIME", None)
OP_INTERVAL = os.environ.get("OP_INTERVAL", None)

fullCmdArguments = sys.argv
argumentList = fullCmdArguments[1:]
unixOptions = "ht:u:p:d:r:m:i:"
gnuOptions = ["help", "type=", "user=", "passwd=", "dbname=", "rate=", "time=", "interval="]

try:
    arguments, values = getopt.getopt(argumentList, unixOptions, gnuOptions)
except getopt.error as err:
    print (str(err))
    sys.exit(2)

for currentArgument, currentValue in arguments:
    if currentArgument in ("-h", "--help"):
        print ("\nusage: hammer.py [-h | -t type | -u user | -p passwd | -d dbname | -r rate | -m time | -i interval]\nOptions and arguments (and corresponding environment variables):\n-d db\t: database name to connect to or create if it doesn't exist\n-h\t: display this help\n-i interval : number of seconds between spikes when using spikey type\n-m time\t: length of time the spike should last when using spikey type\n-p pwd\t: password for the database user\n-r rate\t: times per second data should be read/written to db\n-t type\t: type of workload to use to hammer database\n-u usr\t: database user to connect with\n\nOther environment variables:\nDB_USER\t  : database user to connect with. Overridden by the -u flag\nDB_PASS\t  : database password. Overridden by the -p flag.\nDB_NAME\t  : database to connect to. Overridden by the -d flag.\nWORK_TYPE : pattern of workload to hit the database with\nOP_RATE\t  : times per second data should be read/written to db. Overridden by the -r flag\nOP_TIME\t  : length in secs spike should last when using spikey type. Overridden by the -m flag\nOP_INTERVAL : num seconds between spikes when using spikey type. Overridden by the -i flag")
        sys.exit(0)

    if currentArgument in ("-t", "--type"):
        if currentValue != "consistent" and currentValue != "spikey":
            print ("Only values for type are 'consistent' or 'spikey'")
            sys.exit(2)
    elif currentArgument in ("-u", "--user"):
        DB_USER = currentValue
    elif currentArgument in ("-p", "--passwd"):
        DB_PASS = currentValue
    elif currentArgument in ("-d", "--dbname"):
        DB_NAME = currentValue
    elif currentArgument in ("-r", "--rate"):
        OP_RATE = currentValue
    elif currentArgument in ("-m", "--time"):
        OP_TIME = currentValue
    elif currentArgument in ("-i", "--interval"):
        OP_INTERVAL = currentValue

import poc

try:
    mydb = mysql.connector.connect(
        host="127.0.0.1", # This is because we use the proxy
        user=DB_USER,
        passwd=DB_PASS,
        database=DB_NAME
    )
except Error as e:
    print ("Couldn't connect to the MySQL instance.")
    sys.exit(2)

mycursor = mydb.cursor()

# ~50/second running locally. Need to test this from a K8 cluster of course to proper rate adjust
# Can definitely play with the commit being in the while loop so each insert is discrete transaction. Probably
# need to do that in order to calculate individual commit timings

# These values match the prompt questions in poc.py
READ_UNDEFINED        = -1
READ_CONSISTENT_HIGH  = 1
READ_CONSISTENT_LOW   = 2
READ_SPIKEY           = 3

WRITE_UNDEFINED       = -1
WRITE_CONSISTENT_HIGH = 1
WRITE_CONSISTENT_LOW  = 2
WRITE_SPIKEY          = 3

DATABASE_UNDEFINED    = -1
DATABASE_SQL          = 1
DATABASE_SPANNER      = 2

t = time.time()
spike_state_counter = 0
while True:
    curT = time.time_ns()
    mycursor.execute("INSERT INTO data (random) VALUES ('abcd')")
    mydb.commit()
    curT = time.time_ns() - curT
    print ("{} ms".format(curT/1000000)) # milliseconds
    if poc.get_write_workload() == WRITE_CONSISTENT_LOW:
        time.sleep(1)
    elif poc.get_write_workload() == WRITE_SPIKEY:
        if spike_state_counter > 20:
            spike_state_counter = 0
            time.sleep(3)
        else:
            spike_state_counter = spike_state_counter + 1
