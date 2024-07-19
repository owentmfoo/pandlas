import random
import time

from pandlas.SqlRace import SQLiteConnection
from pandlas import session_frame
from pandlas.utils import timestamp2long
import os
import pandas as pd
import numpy as np
import logging
from tqdm import trange
import requests

logger = logging.getLogger(__name__)

logging.basicConfig(format='%(asctime)s:%(levelname)s:%(name)s:%(message)s', level=logging.INFO)

db_location = r'C:\McLaren Applied\pandlas\ExampleSessions.ssndb'
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
            logger.info("Successful request.")
            df = pd.DataFrame([response.json()])
            df.index = pd.to_datetime(df.timestamp, unit='s')
            df.drop(columns=['visibility', 'name', 'units'], inplace=True)
            df.atlas.to_atlas_session(session, show_progress_bar=False)
        else:
            logger.info("Unsuccessful request.")
        time.sleep(1)
