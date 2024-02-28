"""Pythonized version of common SQLRace calls"""

import os
from abc import ABC, abstractmethod

import clr
import logging
import numpy as np
from pandlas.utils import is_port_in_use

logger = logging.getLogger(__name__)

A10_INSTALL_PATH = r"C:\Program Files\McLaren Applied Technologies\ATLAS 10"
SQL_RACE_DLL_PATH = rf"{A10_INSTALL_PATH}\MESL.SqlRace.Domain.dll"

# Configure Pythonnet and reference the required assemblies for dotnet and SQL Race
clr.AddReference("System.Collections")  # pylint: disable=no-member
clr.AddReference("System.Core")  # pylint: disable=no-member
clr.AddReference("System.IO")  # pylint: disable=no-member

if not os.path.isfile(SQL_RACE_DLL_PATH):
    raise FileNotFoundError(
        "Couldn't find SQL Race DLL at "
        + SQL_RACE_DLL_PATH
        + " please check that Atlas 10 is installed"
    )

clr.AddReference(SQL_RACE_DLL_PATH)

from MAT.OCS.Core import SessionKey  # .NET imports, so pylint: disable=wrong-import-position,wrong-import-order,import-error,wildcard-import
from MESL.SqlRace.Domain import (  # .NET imports, so pylint: disable=wrong-import-position,wrong-import-order,import-error,wildcard-import
    Core,
    SessionManager,
    SessionState,
    RecordersConfiguration,
)
from System import DateTime, Guid  # .NET imports, so pylint: disable=wrong-import-position,wrong-import-order,import-error,wildcard-import
from System.Collections.Generic import List  # .NET imports, so pylint: disable=wrong-import-position,wrong-import-order,import-error,wildcard-import
from System.Net import IPEndPoint, IPAddress  # .NET imports, so pylint: disable=wrong-import-position,wrong-import-order,import-error,wildcard-import


def initialise_sqlrace():
    """Check if SQLRace is initialised and initialise it if not."""
    if not Core.IsInitialized:
        logger.info("Initialising SQLRace API.")
        Core.LicenceProgramName = "SQLRace"
        Core.Initialize()
        logger.info("SQLRace API initialised.")


class SessionConnection(ABC):
    """Abstract class that represents a session connection"""

    initialise_sqlrace()
    sessionManager = SessionManager.CreateSessionManager()  # .NET objects, so pylint: disable=invalid-name

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
    """Represents a connection to a ATLAS session in a SQLite database

    This connections can either be reading from an existing session (mode = 'r') or
    creating a new session (mode = 'w')

    This Class supports the use of contex manager and will close the client session on
    exit.

    """

    def __init__(
        self,
        db_location,
        session_identifier: str = "",
        session_key: str = None,
        mode="r",
        recorder=False,
    ):
        """Initializes a connection to a SQLite ATLAS session.

        Args:
            db_location: Location of SQLite database to connect to.
            session_identifier: Name of the session identifier.
            session_key: Session key of the session, leave it as None if creating a new
                session
            mode: read 'r' or  write 'w'.
            recorder: Only applies in write mode, set to Ture to configure the SQLRace
                Server Listener and  Recorder, so it can be viewed as a live session in
                ATLAS.
        """
        self.client = None
        self.session = None
        self.db_location = db_location
        self.session_identifier = session_identifier
        self.mode = mode
        self.recorder = recorder

        if session_key is not None:
            self.sessionKey = SessionKey.Parse(session_key)  # .NET objects, so pylint: disable=invalid-name
        else:
            self.sessionKey = None

        if self.mode == "r":
            self.load_session(session_key)
        elif self.mode == "w":
            self.create_sqlite()

    @property
    def connection_string(self):
        return f"DbEngine=SQLite;Data Source={self.db_location};Pooling=false;"

    def create_sqlite(self):
        if self.recorder:
            self.start_recorder()
        self.sessionKey = SessionKey.NewKey()
        sessionDate = DateTime.Now  # .NET objects, so pylint: disable=invalid-name
        event_type = "Session"
        logger.debug(
            "Creating new session with connection string %s.", self.connection_string
        )
        clientSession = self.sessionManager.CreateSession(  # .NET objects, so pylint: disable=invalid-name
            self.connection_string,
            self.sessionKey,
            self.session_identifier,
            sessionDate,
            event_type,
        )
        self.client = clientSession
        self.session = clientSession.Session
        logger.info("SQLite session created.")

    def start_recorder(self, port=7300):
        """Configures the SQL Server listener and recorder

        Args:
            port: Port number to open the Server Listener on.

        """
        # Find a port that is not in used
        while is_port_in_use(port):
            port += 1
        logger.info("Opening server lister on port %d.", port)
        # Configure server listener
        Core.ConfigureServer(True, IPEndPoint(IPAddress.Parse("127.0.0.1"), port))
        recorderConfiguration = RecordersConfiguration.GetRecordersConfiguration()  # .NET objects, so pylint: disable=invalid-name
        recorderConfiguration.AddConfiguration(
            Guid.NewGuid(),
            "SQLite",
            self.db_location,
            self.db_location,
            self.connection_string,
            False,
        )
        if self.sessionManager.ServerListener.IsRunning:
            logger.info(
                "Server listener is running: %s.",
                self.sessionManager.ServerListener.IsRunning,
            )
        else:
            logger.warning(
                "Server listener is running: %s.",
                self.sessionManager.ServerListener.IsRunning,
            )
        logger.debug(
            "Configuring recorder with connection string %s.", self.connection_string
        )

    def load_session(self, session_key: str = None):
        """Loads a historic session from the SQLite database

        Args:
            session_key: Optional, updates the sessionKey attribute and opens that
                session.

        Returns:
            None, session is opened and can be accessed from the attribute self.session.
        """
        if session_key is not None:
            self.sessionKey = SessionKey.Parse(session_key)
        elif self.sessionKey is None:
            raise TypeError(
                "load_session() missing 1 required positional argument: 'session_key'"
            )
        self.client = self.sessionManager.Load(self.sessionKey, self.connection_string)
        self.session = self.client.Session

        logger.info("SQLite session loaded.")

    def __enter__(self):
        return self.session


class Ssn2Session(SessionConnection):
    """Represents a session connection to a SSN2 file."""

    def __init__(self, file_location):
        self.sessionKey = None  # .NET objects, so pylint: disable=invalid-name
        self.client = None
        self.session = None
        self.db_location = file_location

    def load_session(self):
        """Loads the session from the SSN2 file."""
        connection_string = f"DbEngine=SQLite;Data Source= {self.db_location}"
        stateList = List[SessionState]()  # .NET objects, so pylint: disable=invalid-name
        stateList.Add(SessionState.Historical)

        # Summary
        summary = self.sessionManager.Find(connection_string, 1, stateList, False)
        if summary.Count != 1:
            logger.warning(
                "SSN2 contains more than 1 session. Loading session %s. Consider "
                "using 'SQLiteConnection' instead and specify the session key.",
                summary.get_Item(0).Identifier,
            )
        self.sessionKey = summary.get_Item(0).Key
        self.client = self.sessionManager.Load(self.sessionKey, connection_string)
        self.session = self.client.Session

        logger.info("SSN2 session loaded.")

    def __enter__(self):
        self.load_session()
        return self.session


class SQLRaceDBConnection(SessionConnection):
    """Represents a connection to a ATLAS session in a SQLRace database

    This connections can either be reading from an existing session (mode = 'r') or
    creating a new session (mode = 'w')

    This class supports the use of contex manager and will close the client session on
    exit.

    """

    def __init__(
        self,
        data_source,
        database,
        session_identifier: str = "",
        session_key: str = None,
        mode="r",
        recorder=False,
    ):
        """Initializes a connection to a SQLite ATLAS session.

        Args:
            data_source: Name or network address of the instance of SQL Server to
                connect to.
            database: Name of the database
            session_identifier: Name of the session identifier.
            session_key: Session key of the session, leave it as None if creating a new
                session.
            mode: read 'r' or  write 'w'.
            recorder: Only applies in write mode, set to Ture to configure the  SQLRace
                Server Listener and  Recorder, so it can be viewed as a live session in
                ATLAS.
        """
        self.client = None
        self.session = None
        self.data_source = data_source
        self.database = database
        self.session_identifier = session_identifier
        self.mode = mode
        self.recorder = recorder

        if session_key is not None:
            self.sessionKey = SessionKey.Parse(session_key)  # .NET objects, so pylint: disable=invalid-name
        else:
            self.sessionKey = None

        if self.mode == "r":
            self.load_session(session_key)
        elif self.mode == "w":
            self.create_sqlrace()

    @property
    def connection_string(self):
        return (
            f"server={self.data_source};Initial Catalog={self.database};"
            f"Trusted_Connection=True;"
        )

    def create_sqlrace(self):
        if self.recorder:
            self.start_recorder()
        self.sessionKey = SessionKey.NewKey()
        sessionDate = DateTime.Now  # .NET objects, so pylint: disable=invalid-name
        event_type = "Session"
        logger.debug(
            "Creating new session with connection string %s.", self.connection_string
        )
        clientSession = self.sessionManager.CreateSession(  # .NET objects, so pylint: disable=invalid-name
            self.connection_string,
            self.sessionKey,
            self.session_identifier,
            sessionDate,
            event_type,
        )
        self.client = clientSession
        self.session = clientSession.Session
        logger.info("SQLRace Database session created.")

    def start_recorder(self, port=7300):
        """Configures the SQL Server listener and recorder

        Args:
            port: Port number to open the Server Listener on.

        """
        # Find a port that is not in used
        while is_port_in_use(port):
            port += 1
        logger.info("Opening server lister on port %d.", port)
        # Configure server listener
        Core.ConfigureServer(True, IPEndPoint(IPAddress.Parse("127.0.0.1"), port))

        # Configure recorder
        recorderConfiguration = RecordersConfiguration.GetRecordersConfiguration()  # .NET objects, so pylint: disable=invalid-name
        recorderConfiguration.AddConfiguration(
            Guid.NewGuid(),
            "SQLServer",
            rf"{self.data_source}\{self.database}",
            rf"{self.data_source}\{self.database}",
            self.connection_string,
            False,
        )
        if self.sessionManager.ServerListener.IsRunning:
            logger.info(
                "Server listener is running: %s.",
                self.sessionManager.ServerListener.IsRunning,
            )
        else:
            logger.warning(
                "Server listener is running: %s.",
                self.sessionManager.ServerListener.IsRunning,
            )
        logger.debug(
            "Configuring recorder with connection string %s.", self.connection_string
        )

    def load_session(self, session_key: str | None):
        """Loads a historic session from the SQLRace database

        Args:
            session_key: Optional, updates the sessionKey attribute and opens that
                session.

        Returns:
            session is opened and can be accessed from the attribute self.session.
        """
        if session_key is not None:
            self.sessionKey = SessionKey.Parse(session_key)
        elif self.sessionKey is None:
            raise TypeError(
                "load_session() missing 1 required positional argument: 'session_key'"
            )
        self.client = self.sessionManager.Load(self.sessionKey, self.connection_string)
        self.session = self.client.Session

        logger.info("SQLRace Database session loaded.")

    def __enter__(self):
        return self.session


def get_samples(
    session, parameter: str, start_time: int = None, end_time: int = None
) -> tuple[np.ndarray, np.ndarray]:
    """Gets all the samples for a parameter in the session

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
    ParameterValues = pda.GetSamplesBetween(start_time, end_time, sample_count)  # .NET objects, so pylint: disable=invalid-name
    return np.array(ParameterValues.Data), np.array(ParameterValues.Timestamp)
