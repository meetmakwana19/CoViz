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
parser.add_argument('-l', '--log', help="Logging Level: --log|-l=[DEBUG | INFO]")
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
# db_folder = 'Databases'
district_id = '395'
request_url = 'https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/'\
                'calendarByDistrict?district_id={}&date={}'
vaccines = {
    'COVISHIELD': 1,
    'COVAXIN': 2,
    'SPUTNIK V': 3
}

previous_response = {}

"""
Logging Setup
Provide logging level - debug/info/error from CLI as 'python main.py --log=DEBUG'
"""
# os.makedirs("/home/VisualizeCoWIN-data/Databases/{}".format(date_time))
os.makedirs("C:\\Users\\Meet\\OneDrive\\Desktop\\CoViz_latest\\Databases\\{}".format(date_time))
# Path(db_folder).mkdir(exist_ok=True)
# os.mkdir(date_time)

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
    'Databases/'+date_time+'/'+date_today+'.log', 
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
Currently configured for running 20 times after every 3 seconds, meaning the
whole process would run for an hour.
For running 24 hours, set loop_times = 20*60*24
"""
loop_times = 20*60*24
loop_interval = 3.0
_loopCounter = 0

"""
SQLite3 Database Setup/Connection
"""
database = '{}.db'.format(date_time)
connection = sqlite3.connect('Databases/{}/{}'.format(date_time, database))
cursor = connection.cursor()
try:
    logger.debug('Attempting to create tables.')
    # cursor.execute('''CREATE TABLE '{}' (id integer PRIMARY KEY, date_time text, 
        # center_id int, session_id text, session_date text, vaccine int, dose1 int, 
        # dose2 int, min_age int, max_age int)'''.format(date_today))
    cursor.execute('''CREATE TABLE trend (id integer PRIMARY KEY, date_time text, 
        center_id int, session_id text, session_date text, vaccine int, dose1 int, 
        dose2 int, min_age int, max_age int)''')
    connection.commit()
    logger.info('Tables created.')
except sqlite3.Error as e:
    logger.error('Couldn\'t create table: {}'.format(e))
    

def get_previous_dose_quantity(session_id):
    return previous_response.get(session_id, None)

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
            if 'sessions' in centre:
                for session in centre['sessions']:
                    # Assuming that available_capacity will always be present
                    # for proper records
                    if 'available_capacity' in session:
                        max_age = session.get('max_age_limit', None)
                        vaccine_id = vaccines[session['vaccine']]
                        entry = (
                            current_time, centre['center_id'], session['session_id'],
                            session['date'], vaccine_id,
                            session['available_capacity_dose1'],
                            session['available_capacity_dose2'],
                            session['min_age_limit'],
                            max_age,
                        )
                        entries.append(entry)
                        """
                        session_id is used as a key in a dictionary named 'previous_response' - line 41 & 149
                        The value {dose1: x, dose2: y} is mapped to the session_id key in this dict
                        So we can check the previous value as: previous_response[<session_id>].
                        By comparing previous values from the previous_response dict and new values from the
                        new response - line 147 to 152, we can decide if the values changed.
                        """
                        previous_dose_quantity = get_previous_dose_quantity(session['session_id'])
                        # If this session_id has appeared for the first time, previous_dose_quantity
                        # will be None, hence, it is important to check if it is not None
                        if previous_dose_quantity:
                            # If it is not none, we can start comparing previous and new values
                            if previous_dose_quantity['dose1'] != session['available_capacity_dose1'] or \
                                previous_dose_quantity['dose2'] != session['available_capacity_dose2']:
                                # if values are not equal i.e. they changed, append new values defined as 'entry' - line 131
                                # to a list called 'changed_entries'. All tuples in changed_entries will
                                # be inserted into the 'trend' table.
                                logger.debug('Change: {}'.format(entry))
                                changed_entries.append(entry)
                        elif previous_dose_quantity is None:
                            # Although if previous_dose_quantity is None, this session must have been added
                            # between this fetch and the previous fetch, hence put this entry in changed_entries as well.
                            changed_entries.append(entry)
                        # Update the previous_response value for this sessions, before moving on to the next session
                        previous_response[session['session_id']] = {
                            'dose1': session['available_capacity_dose1'],
                            'dose2': session['available_capacity_dose2']
                        }
    except requests.exceptions.HTTPError as e:
        logger.error('HTTPError: {}'.format(e))
    except requests.exceptions.Timeout as e:
        logger.error('Request timed out')
    except requests.exceptions.RequestException as e:
        logger.error('RequestException: {}'.format(e))
    try:
        # if len(entries) > 0:
            # cursor.executemany('''INSERT INTO '{}' (date_time, center_id, session_id, 
                # session_date, vaccine, dose1, dose2, min_age, max_age) 
                # VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'''.format(date_today), entries)
            # connection.commit()
        if len(changed_entries) > 0:
            cursor.executemany('''INSERT INTO trend (date_time, center_id, session_id, 
                session_date, vaccine, dose1, dose2, min_age, max_age) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', changed_entries)
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
