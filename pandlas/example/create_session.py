"""
Example to create a session with multiple data rate and merging configs
"""

import logging
import pandas as pd
import numpy as np
import pandlas.SqlRace as sr

logging.basicConfig(level=logging.INFO)

SQLITE_DB_DIR = r'c:\temp\pandlas\temp.ssndb'

start = np.datetime64("now")

# the minimum to add data to a session:
#   - dataframe with a datetime index
#   - a column with data in doubles
df = pd.DataFrame()
df.index = pd.date_range(start, periods=1000, freq='S')
df.loc[:, "Param 1"] = np.sin(np.linspace(0, 10 * np.pi, num=1000))

# some optional extras
#   - change the app group name
#   - change the parameter group name
#   - disable the progress bars
df2 = pd.DataFrame()
df2.index = pd.date_range(start, periods=100, freq='10S')
df2.loc[:, "Param 2"] = np.sin(np.linspace(0, 10 * np.pi, num=100))
df2.atlas.ParameterGroupIdentifier = "Sub group 1"
df2.atlas.ApplicationGroupName = "AppGroup2"
df2.atlas.descriptions = {"Param 2:AppGroup2":"Custom Description"}
df2.atlas.display_format = {"Param 2:AppGroup2":"%5.2f"}
df2.atlas.units = {"Param 2:AppGroup2":"m/s"}

session_identifier = "TestSession"
with sr.SQLiteConnection(SQLITE_DB_DIR, session_identifier, mode='w') as session:
    df.atlas.to_atlas_session(session)
    df2.atlas.to_atlas_session(session, show_progress_bar=False)
