from twisted.internet import task
from twisted.internet import reactor
import sqlite3
from datetime import date, datetime
import requests
import sys
import os
import json
import logging
import argparse
from pathlib import Path

"""
Argument parser to get logging level from the command line
"""
parser = argparse.ArgumentParser()
parser.add_argument('-l', '--log', help="Logging Level: DEBUG, INFO")
args = parser.parse_args()
if args.log:
    loglevel = args.log
else:
    loglevel = 'INFO'

"""
Today's date used as filename for table and log file.
Mumbai's district_id is 395.
Request URL is the CoWIN API calendarByDistrict endpoint
which needs a district id and date parameter.
Vaccine names are mapped to integers.
"""
date_today = date.today().strftime('%d-%m-%y')
date_time = str(datetime.now().strftime("%d-%m-%y %H-%M-%S"))
centres_log_folder = 'centres_logs'
district_id = '395'
request_url = 'https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/'\
                'calendarByDistrict?district_id={}&date={}'
vaccines = {
    'COVISHIELD': 1,
    'COVAXIN': 2,
    'SPUTNIK V': 3
}

previous_fees = {}
known_centres = []
"""
Logging Setup
Provide logging level - debug/info/error from CLI as 'python main.py --log=DEBUG'
"""
# os.mkdir(centres_log_folder)
Path(centres_log_folder).mkdir(exist_ok=True)

numeric_level = getattr(logging, loglevel.upper(), None)
if not isinstance(numeric_level, int):
    raise ValueError('Invalid log level: %s' % loglevel)
    
logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    '%(asctime)s %(levelname)-8s %(message)s'
)
logger.setLevel(logging.DEBUG)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(numeric_level)
stream_handler.setFormatter(formatter)

file_handler = logging.FileHandler(
    centres_log_folder+'/'+date_today+'.log', 
    mode='w', 
    encoding='utf-8'
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

logger.addHandler(stream_handler)
logger.addHandler(file_handler)

"""
Parameters to control the number of times to fetch and store the data, and at
what interval to do so.
Currently configured for running 60 times after every 60 seconds, meaning the
whole process would run for an hour.
For running 24 hours, set loop_times = 60*24
"""
# Run once every 10 minutes until 6*24 runs
loop_times = 6*24
loop_interval = 600.0
_loopCounter = 0

"""
SQLite3 Database Connection Setup
"""
database = 'center.db'
connection = sqlite3.connect(database)
try:
    cursor = connection.cursor()
    logger.debug('Attempting to create tables.')
    cursor.execute('''CREATE TABLE IF NOT EXISTS centers (
        Center_ID int,
        Name text,
        Address text,
        State_Name text,
        District_Name text,
        Block_Name text,
        Pincode text,
        Latitude float,
        Longitude float
        )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS fee_trend (
        Center_ID int,
        Name text,
        Covishield int,
        Covaxin int,
        SputnikV int)''')
    connection.commit()
    logger.info('Tables created.')
except sqlite3.Error as e:
    logger.error('Couldn\'t create table: {}'.format(e))

# This will get previously stored data in the db
# notice how we also used IF NOT EXISTS before while creating tables which will only create table sif they don't exist
# and do nothing if they exist
try:
    cursor = connection.cursor()
    cursor.execute('''SELECT Center_ID from centers''')
    centers = cursor.fetchall()
    known_centres = [center[0] for center in centers]
    cursor.execute('''SELECT * from fee_trend''')
    fees = cursor.fetchall()
    # fees will be a list of tuples like [(4545, 780, Null, Null), (4545, 780, 1250, Null)]
    for fee in fees:
        previous_fees[fee[0]] = {
            'Covishield': fee[2],
            'Covaxin': fee[3],
            'SputnikV': fee[4]
        }
except sqlite3.error as e:
    logger.error('Couldn\t fetch existing centers from database')

def get_previous_fees(center_id):
    return previous_fees.get(center_id, None)

def get_data():
    """
    Sends a GET request to the calendarByDistrict endpoint, transforms
    and stores the data in the database.
    The response contains center details as well which would be stored
    in another table and so they are omitted here. We have a foreign key
    center_id.
    """
    global _loopCounter
    if _loopCounter == loop_times:
        loop.stop()
        return
    _loopCounter += 1
    # All entries fetched within one request will have this timestamp
    # current_fetch_time = datetime.now()
    current_time = datetime.now().replace(microsecond=0).isoformat()
    # if previous_fetch_time == None:
    # previous_fetch_time = current_fetch_time
    
    entries = []
    changed_entries = []
    new_centres = []
    try:
        r = requests.get(request_url.format(district_id, date_today))
        logger.debug('Request sent: GET {}'.format(r.url))
        r.raise_for_status()
        
        if r.status_code == 200:
            logger.debug('Request successful: {} {}'.format(r.status_code, r.reason)) 
        if r.status_code != 200:
            logger.warning('Request not successful: {} {}'.format(r.status_code, r.reason))
        if not r.ok:
            logger.warning('Request failed: {} {}'.format(r.status_code, r.reason))
            
        response = r.json()
        if 'centers' not in response:
            logger.debug('Empty response: {}'.format(response))
            return
        for centre in response['centers']:
            """
            Center duplication prevention
            """
            if centre['center_id'] not in known_centres:
                known_centres.append(centre['center_id'])
                new_centre = (centre['center_id'], centre['name'], centre['address'],
                    centre['state_name'], centre['district_name'], centre['block_name'],
                    centre['pincode'], centre['lat'], centre['long'])
                logger.debug('New center: {}'.format(new_centre))
                new_centres.append(new_centre)
            # Nice check
            if 'fee_type' in centre:
                covishield_fee = None
                covaxin_fee = None
                sputnik_fee = None
                if centre["fee_type"] == "Free":
                    for session in centre['sessions']:
                        if session['vaccine']=='COVISHIELD':
                            covishield_fee = 0
                        elif session['vaccine']=='COVAXIN':
                            covaxin_fee = 0
                        elif session['vaccine']=='SPUTNIK V':
                            sputnik_fee = 0
                elif centre["fee_type"] == "Paid":
                    """What I changed"""
                    # A tuple was being compared with None
                    # Packed the tuple while intializing the entry variable
                    # covishield_fee = (None, )
                    # covaxin_fee = (None, )
                    # sputnik_fee = (None, )
                    for vaccine_fee in centre['vaccine_fees']:
                        # Check if the vaccine is there otherwise set its fee to None
                        if vaccine_fee['vaccine']=='COVISHIELD':
                            covishield_fee = int(vaccine_fee['fee'])
                        elif vaccine_fee['vaccine']=='COVAXIN':
                            covaxin_fee = int(vaccine_fee['fee'])
                        elif vaccine_fee['vaccine']=='SPUTNIK V':
                            sputnik_fee = int(vaccine_fee['fee'])
                entry = (centre['center_id'], centre['name'], covishield_fee, covaxin_fee, sputnik_fee, )
                """Change over"""
                previous_fee = get_previous_fees(centre['center_id'])
                if previous_fee:
                    if covishield_fee != previous_fee['Covishield'] or\
                        covaxin_fee != previous_fee['Covaxin'] or\
                        sputnik_fee != previous_fee['SputnikV']:
                        changed_entries.append(entry)
                        logger.debug('Fee change: {}'.format(entry))
                elif previous_fee is None:
                    changed_entries.append(entry)
                previous_fees[centre['center_id']] = {
                    'Covishield': covishield_fee,
                    'Covaxin': covaxin_fee,
                    'SputnikV': sputnik_fee
                }
    except requests.exceptions.HTTPError as e:
        logger.error('HTTPError: {}'.format(e))
    except requests.exceptions.Timeout as e:
        logger.error('Request timed out')
    except requests.exceptions.RequestException as e:
        logger.error('RequestException: {}'.format(e))
    try:
        if len(new_centres) > 0:
            cursor.executemany('''INSERT INTO centers 
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)''', new_centres)
            connection.commit()
        if len(changed_entries) > 0:
            cursor.executemany('''INSERT INTO fee_trend (
                Center_ID,
                Name, 
                Covishield,
                Covaxin,
                SputnikV) 
                VALUES(?, ?, ?, ?, ?)''', changed_entries)
            connection.commit()
    except sqlite3.Error as e:
        logger.error('Couldn\'t insert into table: {}'.format(e))


def cbLoopDone(result):
    """
    Called when loop was stopped with success.
    """
    logger.info('Done for today! ({})'.format(date_today))
    reactor.stop()


def ebLoopFailed(failure):
    """
    Called when loop execution failed.
    """
    logger.error('FATAL: {}'.format(failure.getBriefTraceback()))
    reactor.stop()


loop = task.LoopingCall(get_data)

logger.info('Requesting data from CoWIN API')
loopDeferred = loop.start(loop_interval)

# Add callbacks for stop and failure.
loopDeferred.addCallback(cbLoopDone)
loopDeferred.addErrback(ebLoopFailed)

reactor.run()
