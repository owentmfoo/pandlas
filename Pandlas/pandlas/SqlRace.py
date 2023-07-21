"""Pythonized version of common SQLRace calls"""
import os
import clr
import logging

# The path to the main SQL Race DLL. This is the default location when installed with Atlas 10
sql_race_dll_path = r"C:\Program Files\McLaren Applied Technologies\ATLAS 10\MESL.SqlRace.Domain.dll"
ssn2splitter_dll_path = r"C:\Program Files\McLaren Applied Technologies\ATLAS 10\MAT.SqlRace.Ssn2Splitter.dll"
# The paths to Automation API DLL files.
automation_api_dll_path = r"C:\Program Files\McLaren Applied Technologies\ATLAS 10\MAT.Atlas.Automation.Api.dll"
automation_client_dll_path = r"C:\Program Files\McLaren Applied Technologies\ATLAS 10\MAT.Atlas.Automation.Client.dll"

# Configure Pythonnet and reference the required assemblies for dotnet and SQL Race
clr.AddReference("System.Collections")
clr.AddReference("System.Core")
clr.AddReference("System.IO")

if not os.path.isfile(sql_race_dll_path):
    raise Exception("Couldn't find SQL Race DLL at " + sql_race_dll_path + " please check that Atlas 10 is installed")

clr.AddReference(sql_race_dll_path)

if not os.path.isfile(automation_api_dll_path):
    raise Exception(f"Couldn't find Automation API DLL at {automation_api_dll_path}.")

clr.AddReference(automation_api_dll_path)

if not os.path.isfile(automation_client_dll_path):
    raise Exception(f"Couldn't find Automation Client DLL at {automation_client_dll_path}.")
clr.AddReference(automation_client_dll_path)

from MAT.OCS.Core import SessionKey
from MESL.SqlRace.Domain import Core, SessionManager
from System import DateTime


def initialise_sqlrace():
    """Check if SQLRace is initialised and initialise it if not."""
    if not Core.IsInitialized:
        logging.info('Initialising SQLRace API')
        Core.LicenceProgramName = "SQLRace"
        Core.Initialize()
        logging.info('SQLRace API Initialised')


class sessionConnection:
    """Represents a SQL session connection"""
    initialise_sqlrace()
    sessionManager = SessionManager.CreateSessionManager()

    def __init__(self, db_location, sessionIdentifier, db_engine='SQLite'):
        self.client = None
        self.session = None
        self.db_location = db_location
        self.sessionIdentifier = sessionIdentifier
        self.db_engine = db_engine

    def create_sqlite(self):
        connectionString = rf"DbEngine=SQLite;Data Source={self.db_location};"
        sessionKey = SessionKey.NewKey()
        sessionDate = DateTime.Now
        event_type = 'Session'
        clientSession = self.sessionManager.CreateSession(connectionString, sessionKey, self.sessionIdentifier,
                                                          sessionDate, event_type)
        self.client = clientSession
        self.session = clientSession.Session
        logging.info('SQLite session created')

    def __enter__(self):
        if self.db_engine == 'SQLite':
            self.create_sqlite()
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.Close()
        logging.info('Session closed.')