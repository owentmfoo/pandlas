"""
Example to create a session with multiple data rate and merging configs
"""

from pandlas.SqlRace import initialise_sqlrace
from pandlas import session_frame
from pandlas.session_frame import SessionManager, SessionKey, DateTime
import os
import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)

initialise_sqlrace()

litedbdir = r'c:\temp\pandlas\temp.ssndb'
pathToFile = r'c:\ssn2\test1.ssn2'
pathToFile = os.path.abspath(pathToFile)
output_dir = os.path.dirname(pathToFile)

#  Create new session
connectionString = rf"DbEngine=SQLite;Data Source={litedbdir};"
sessionManager = SessionManager.CreateSessionManager()
sessionKey = SessionKey.NewKey()
sessionIdentifier = os.path.basename(pathToFile).strip(".ssn2")
sessionDate = DateTime.Now
eventType = 'Session'
clientSession = sessionManager.CreateSession(connectionString, sessionKey, sessionIdentifier,
                                             sessionDate, eventType)
session = clientSession.Session
logging.info('Session created')

start = np.datetime64("now")

# the minimum to add data to a session:
#   - dataframe with a datetime index
#   - a column with data in doubles
df = pd.DataFrame()
df.index = pd.date_range(start, periods=1000, freq='S')
df.loc[:, "Param 1"] = np.sin(np.linspace(0, 10 * np.pi, num=1000))
df.atlas.to_ssn2(session)


# some optional extras
#   - change the app group name
#   - change the parameter group name
#   - disable the progress bars
df2 = pd.DataFrame()
df2.index = pd.date_range(start, periods=100, freq='10S')
df2.loc[:, "Param 2"] = np.sin(np.linspace(0, 10 * np.pi, num=100))
df2.atlas.ParameterGroupIdentifier = "Sub group 1"
df2.atlas.ApplicationGroupName = "App Group2"
df2.atlas.to_ssn2(session, show_progress_bar=False)

clientSession.Close()
