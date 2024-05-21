import datetime
import random
import time

from pythonnet import load
load("coreclr", runtime_config=r"C:\Program Files\McLaren Applied Technologies\ATLAS 10\MAT.Atlas.Host.runtimeconfig.json")
import clr
from pandlas.SqlRace import SQLiteConnection, initialise_sqlrace
from pandlas import session_frame
# from pandlas.session_frame import SessionManager, SessionKey, DateTime, Lap, Byte, Marker, MarkerLabel
# from pandlas.utils import timestamp2long
import os
import pandas as pd
import numpy as np
import logging
from tqdm import trange
import requests


logging.basicConfig(level=logging.INFO)

initialise_sqlrace()

logging.basicConfig(level=logging.INFO)

db_location = r'C:\temp\pandlas\lite.ssndb'
session_identifier = 'Live ISS Demo'

cols = ['sin', 'cos']

WTIA_ENDPOINT = r"https://api.wheretheiss.at/v1/satellites/25544"
# Open the session with the recorder set to true to enable live.
with SQLiteConnection(db_location, session_identifier, mode='w', recorder=True) as session:
    # write data to the session as you would in historic, keeping the same column names.
    # pandlas would use the column names and app name to write the data to the right channel.
    while True:
        response = requests.get(WTIA_ENDPOINT)
        if response.status_code == 200:
            print("Update", datetime.datetime.now().time())
            df = pd.DataFrame([response.json()])
            df.index = pd.to_datetime(df.timestamp,unit='s')
            df.drop(columns=['visibility','name','units'],inplace=True)
            df.atlas.to_atlas_session(session, show_progress_bar=False)
        else: print ("Waiting", datetime.datetime.now().time())
        time.sleep(1)