"""Example to open a live session, and write some data to it"""
import time
import logging
import numpy as np
import pandas as pd
from pandlas.SqlRace import SQLiteConnection
from tqdm import trange

logging.basicConfig(level=logging.INFO)

db_location = r'C:\temp\pandlas\lite.ssndb'
session_identifier = 'Live Session Demo'

cols = ['sin', 'cos']

# Open the session with the recorder set to true to enable live.
with SQLiteConnection(db_location, session_identifier, mode='w', recorder=True) as session:
    # write data to the session as you would in historic, keeping the same column names.
    # pandlas would use the column names and app name to write the data to the right channel.
    for i in trange(1200):
        now = pd.Timestamp.now()
        df = pd.DataFrame(data=[[np.sin(i / 100), np.cos(i / 100)]], index=[now], columns=cols)
        df.atlas.to_atlas_session(session, show_progress_bar=False)
        time.sleep(0.1)
