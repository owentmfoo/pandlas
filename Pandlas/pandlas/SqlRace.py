"""Pythonized version of common SQLRace calls"""
import os
from abc import ABC, abstractmethod

import clr
import logging
import numpy as np
from pandlas.utils import is_port_in_use

logger = logging.getLogger(__name__)
# The path to the main SQL Race DLL. This is the default location when installed with Atlas 10
sql_race_dll_path = (
    r"C:\Program Files\McLaren Applied Technologies\ATLAS 10\MESL.SqlRace.Domain.dll"
)
ssn2splitter_dll_path = r"C:\Program Files\McLaren Applied Technologies\ATLAS 10\MAT.SqlRace.Ssn2Splitter.dll"
# The paths to Automation API DLL files.
automation_api_dll_path = r"C:\Program Files\McLaren Applied Technologies\ATLAS 10\MAT.Atlas.Automation.Api.dll"
automation_client_dll_path = r"C:\Program Files\McLaren Applied Technologies\ATLAS 10\MAT.Atlas.Automation.Client.dll"

# Configure Pythonnet and reference the required assemblies for dotnet and SQL Race
clr.AddReference("System.Collections")
clr.AddReference("System.Core")
clr.AddReference("System.IO")

if not os.path.isfile(sql_race_dll_path):
    raise Exception(
        "Couldn't find SQL Race DLL at "
        + sql_race_dll_path
        + " please check that Atlas 10 is installed"
    )

clr.AddReference(sql_race_dll_path)

if not os.path.isfile(automation_api_dll_path):
    raise Exception(f"Couldn't find Automation API DLL at {automation_api_dll_path}.")

clr.AddReference(automation_api_dll_path)

if not os.path.isfile(automation_client_dll_path):
    raise Exception(
        f"Couldn't find Automation Client DLL at {automation_client_dll_path}."
    )
clr.AddReference(automation_client_dll_path)

from MAT.OCS.Core import SessionKey
from MESL.SqlRace.Domain import Core, SessionManager, FileSessionManager, SessionState, RecordersConfiguration
from System import DateTime, Guid
from System.Collections.Generic import List
from System.Net import IPEndPoint, IPAddress


def initialise_sqlrace():
    """Check if SQLRace is initialised and initialise it if not."""
    if not Core.IsInitialized:
        logger.info("Initialising SQLRace API")
        Core.LicenceProgramName = "SQLRace"
        Core.Initialize()
        logger.info("SQLRace API Initialised")


class SessionConnection(ABC):
    """Abstract class that represents a session connection"""

    initialise_sqlrace()
    sessionManager = SessionManager.CreateSessionManager()

    @abstractmethod
    def __init__(self):
        self.client = None
        raise NotImplementedError

    @abstractmethod
    def __enter__(self):
        raise NotImplementedError

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.Session.EndData()
        self.client.Close()
        logger.info("Session closed.")


class SQLiteConnection(SessionConnection):
    def __init__(
        self, db_location, sessionIdentifier, session_key: str = None, mode="r",recorder = False
    ):
        self.client = None
        self.session = None
        self.db_location = db_location
        self.sessionIdentifier = sessionIdentifier
        self.mode = mode
        self.recorder = recorder
        if not (session_key is None):
            self.sessionKey = SessionKey.Parse(session_key)
        else:
            self.sessionKey = None

    def create_sqlite(self):
        if self.recorder:
            self.start_recorder()
        connectionString = f"DbEngine=SQLite;Data Source={self.db_location};Pooling=false;"
        self.sessionKey = SessionKey.NewKey()
        sessionDate = DateTime.Now
        event_type = "Session"
        clientSession = self.sessionManager.CreateSession(
            connectionString,
            self.sessionKey,
            self.sessionIdentifier,
            sessionDate,
            event_type,
        )
        self.client = clientSession
        self.session = clientSession.Session
        logger.info("SQLite session created")

    def start_recorder(self):
        # Find a port that is not in used
        port = 7300
        while is_port_in_use(port):
            port += 1

        # Configure server listener
        Core.ConfigureServer(True, IPEndPoint(IPAddress.Parse("127.0.0.1"), port))
        connectionString = f"DbEngine=SQLite;Data Source={self.db_location};Pooling=false;"
        recorderConfiguration = RecordersConfiguration.GetRecordersConfiguration()
        recorderConfiguration.AddConfiguration(Guid.NewGuid(), "SQLite", self.db_location, self.db_location,
                                               connectionString, False)

    def load_session(self, session_key: str = None):
        if SessionKey is not None:
            self.sessionKey = SessionKey.Parse(session_key)
        elif self.sessionKey is None:
            raise TypeError(
                "load_session() missing 1 required positional argument: 'session_key'"
            )
        connectionString = f"DbEngine=SQLite;Data Source= {self.db_location}"
        stateList = List[SessionState]()
        stateList.Add(SessionState.Historical)

        # Summary
        summary = self.sessionManager.Find(connectionString, 1, stateList, False)
        key = summary.get_Item(0).Key
        self.client = self.sessionManager.Load(key, connectionString)
        self.session = self.client.Session

        logger.info("SQLite session loaded")

    def __enter__(self):
        if self.mode == "r":
            self.load_session()
        elif self.mode == "w":
            self.create_sqlite()
        return self.session


class Ssn2Session(SessionConnection):
    """Represents a session connection to a SSN2 file."""

    def __init__(self, file_location):
        self.sessionKey = None
        self.client = None
        self.session = None
        self.db_location = file_location

    def load_session(self):
        connectionString = f"DbEngine=SQLite;Data Source= {self.db_location}"
        stateList = List[SessionState]()
        stateList.Add(SessionState.Historical)

        # Summary
        summary = self.sessionManager.Find(connectionString, 1, stateList, False)
        if summary.Count != 1:
            logger.warning(
                "SSN2 contains more than 1 session. Loading session %s. Consider using 'SQLiteConnection' "
                "instead and specify the session key.",
                summary.get_Item(0).Identifier,
            )
        self.sessionKey = summary.get_Item(0).Key
        self.client = self.sessionManager.Load(self.sessionKey, connectionString)
        self.session = self.client.Session

        logger.info("SSN2 session loaded")

    def __enter__(self):
        self.load_session()
        return self.session


def get_samples(
    session, parameter: str, start_time: int = None, end_time: int = None
) -> tuple[np.ndarray, np.ndarray]:
    """Get all the samples for a parameter in the session

    Args:
        session: MESL.SqlRace.Domain.Session object.
        parameter: The parameter identifier.
        start_time: Start time to get samples between in int64.
        end_time: End time to get samples between in int64

    Returns:
        tuple of numpy array of samples, timestamps
    """
    if start_time is None:
        start_time = session.StartTime
    if end_time is None:
        end_time = session.EndTime
    pda = session.CreateParameterDataAccess(parameter)
    sample_count = pda.GetSamplesCount(start_time, end_time)
    ParameterValues = pda.GetSamplesBetween(start_time, end_time, sample_count)
    return np.array(ParameterValues.Data), np.array(ParameterValues.Timestamp)
