"""Pythonized version of common SQLRace calls"""

import os
import math
import random
import functools
from abc import ABC, abstractmethod
from typing import Union
import logging
import pandas as pd
import numpy as np
from tqdm import tqdm
from pandlas.utils import is_port_in_use, timestamp2long

A10_INSTALL_PATH = r"C:\Program Files\McLaren Applied Technologies\ATLAS 10"
# configure pythonnet runtime for SQLRace API
os.environ["PYTHONNET_RUNTIME"] = "coreclr"
os.environ["PYTHONNET_CORECLR_RUNTIME_CONFIG"] = (
    rf"{A10_INSTALL_PATH}\MAT.Atlas.Host.runtimeconfig.json"
)

# only import clr after the runtime has been configured, so pylint: disable=wrong-import-order,wrong-import-position
import clr


logger = logging.getLogger(__name__)

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

# .NET imports, so pylint: disable=wrong-import-position,wrong-import-order,import-error,wildcard-import
from MAT.OCS.Core import (
    SessionKey,
    DataStatusType,
)

# .NET imports, so pylint: disable=wrong-import-position,wrong-import-order,import-error,wildcard-import
from MESL.SqlRace.Domain import (
    Core,
    SessionManager,
    SessionState,
    RecordersConfiguration,
    Session,
    Lap,
    Marker,
    ConfigurationSetManager,
    ParameterGroup,
    ApplicationGroup,
    RationalConversion,
    TextConversion,
    EventDefinition,
    ConfigurationSetAlreadyExistsException,
    ConfigurationSet,
    Parameter,
    Channel,
)

# .NET imports, so pylint: disable=wrong-import-position,wrong-import-order,import-error,wildcard-import
from System import (
    DateTime,
    Guid,
    Byte,
    Double,
    String,
    UInt32,
    Array,
    Int64,
)

# .NET imports, so pylint: disable=wrong-import-position,wrong-import-order,import-error,wildcard-import
from System.Collections.Generic import (
    List,
    List as NETList,
)

# .NET imports, so pylint: disable=wrong-import-position,wrong-import-order,import-error,wildcard-import
from System.Net import (
    IPEndPoint,
    IPAddress,
)

# .NET imports, so pylint: disable=wrong-import-position,wrong-import-order,import-error,wildcard-import
from MESL.SqlRace.Enumerators import DataType, ChannelDataSourceType, EventPriorityType, DeleteSessionOption


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
    # .NET objects, so pylint: disable=invalid-name
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
        ip_address: str = "127.0.0.1",
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
            ip_address: Set by default to the local ip address "127.0.0.1". Modify to
                make it accessible from other instances.
        """
        self.client = None
        self.session = None
        self.db_location = db_location
        self.session_identifier = session_identifier
        self.mode = mode
        self.recorder = recorder
        self.ip_address = ip_address

        if session_key is not None:
            # .NET objects, so pylint: disable=invalid-name
            self.sessionKey = SessionKey.Parse(session_key)
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
        # .NET objects, so pylint: disable=invalid-name
        clientSession = self.sessionManager.CreateSession(
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
        Core.ConfigureServer(True, IPEndPoint(IPAddress.Parse(self.ip_address), port))
        logger.info("Server Listener IPAddress %s.", self.ip_address)
        # .NET objects, so pylint: disable=invalid-name
        recorderConfiguration = RecordersConfiguration.GetRecordersConfiguration()
        recorderConfiguration.AddConfiguration(
            Guid.NewGuid(),
            "SQLite",
            self.db_location,
            self.db_location,
            self.connection_string,
            DeleteSessionOption.NoSessionDelete,
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
        connection_string = f"DbEngine=SQLite;Data Source={self.db_location}"
        # .NET objects, so pylint: disable=invalid-name
        stateList = List[SessionState]()
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
        ip_address: str = "127.0.0.1",
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
            ip_address: Set by default to the local ip address "127.0.0.1". Modify to
                make it accessible from other instances.
        """
        self.client = None
        self.session = None
        self.data_source = data_source
        self.database = database
        self.session_identifier = session_identifier
        self.mode = mode
        self.recorder = recorder
        self.ip_address = ip_address

        if session_key is not None:
            # .NET objects, so pylint: disable=invalid-name
            self.sessionKey = SessionKey.Parse(session_key)
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
        # .NET objects, so pylint: disable=invalid-name
        clientSession = self.sessionManager.CreateSession(
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
        Core.ConfigureServer(True, IPEndPoint(IPAddress.Parse(self.ip_address), port))
        logger.info("Server Listener IPAddress %s.", self.ip_address)
        # Configure recorder
        # .NET objects, so pylint: disable=invalid-name
        recorderConfiguration = RecordersConfiguration.GetRecordersConfiguration()
        recorderConfiguration.AddConfiguration(
            Guid.NewGuid(),
            "SQLServer",
            rf"{self.data_source}\{self.database}",
            rf"{self.data_source}\{self.database}",
            self.connection_string,
            DeleteSessionOption.NoSessionDelete,
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
    pda.GoTo(start_time)
    # .NET objects, so pylint: disable=invalid-name
    parameterValues = pda.GetNextSamples(sample_count)
    data = [
        d
        for (d, s) in zip(parameterValues.Data, parameterValues.DataStatus)
        if s == DataStatusType.Sample
    ]
    timestamps = [
        t
        for (t, s) in zip(parameterValues.Timestamp, parameterValues.DataStatus)
        if s == DataStatusType.Sample
    ]
    return np.array(data), np.array(timestamps)


def add_lap(
    session: Session,
    timestamp: pd.Timestamp,
    lap_number: int = None,
    lap_name: str = None,
    count_for_fastest_lap: bool = True,
) -> None:
    """Add a new lap to the session.

    Args:
        session: MESL.SqlRace.Domain.Session to add the lap to.
        timestamp: Timestamp to add the lap.
        lap_number: Lap number. Default to be `Session.LapCollection.Count + 1`
        lap_name: Lap name. Default to be "Lap {lap_number}".
        count_for_fastest_lap: True if the lap should be considered as part of the
            fastest lap calculation (e.g. a timed lap). Default to be True.

    Returns:
        None
    """
    if lap_number is None:
        lap_number = session.LapCollection.Count + 1
        logger.debug("No lap number provided, set lap number as %i", lap_number)

    if lap_name is None:
        lap_name = f"Lap {lap_number}"
        logger.debug("No lap name provided, set lap name as %s", lap_name)

    newlap = Lap(
        int(timestamp2long(timestamp)),
        lap_number,
        Byte(0),
        lap_name,
        count_for_fastest_lap,
    )
    session.LapCollection.Add(newlap)
    logger.info('Lap "%s" with number %i added at %s', lap_name, lap_number, timestamp)


def update_lap(
    session: Session,
    lap_index: int,
    new_name: str = None,
    new_count_for_fastest: bool = None,
) -> None:
    """Update an existing lap in the session.

    Retrieves the lap at ``lap_index``, modifies the requested fields,
    and calls ``session.LapCollection.Update(lap)`` to persist the change.

    Args:
        session: MESL.SqlRace.Domain.Session containing the lap.
        lap_index: Zero-based index in ``session.LapCollection``.
        new_name: New name for the lap.  ``None`` keeps the current name.
        new_count_for_fastest: New value for the CountForFastestLap flag.
            ``None`` keeps the current value.

    Raises:
        IndexError: If ``lap_index`` is out of range.
    """
    lap_count = session.LapCollection.Count
    if lap_index < 0 or lap_index >= lap_count:
        raise IndexError(
            f"lap_index {lap_index} out of range (0..{lap_count - 1})"
        )

    lap = session.LapCollection[lap_index]
    old_name = lap.Name

    if new_name is not None:
        lap.Name = new_name
    if new_count_for_fastest is not None:
        lap.CountForFastestLap = new_count_for_fastest

    session.LapCollection.Update(lap)
    logger.info(
        'Lap at index %d updated: "%s" -> "%s"',
        lap_index,
        old_name,
        lap.Name,
    )


def add_point_marker(
    session: Session,
    marker_time: pd.Timestamp,
    marker_label: str,
) -> None:
    """Adds a point marker to the session.

    Args:
        session: MESL.SqlRace.Domain.Session to add the point marker to.
        marker_time: Time of the marker.
        marker_label: Label of the marker.
    """
    marker_time = timestamp2long(marker_time)
    newPointMarker = Marker(  # .NET objects, so pylint: disable=invalid-name
        int(marker_time), marker_label
    )
    newMarkers = Array[Marker](  # .NET objects, so pylint: disable=invalid-name
        [newPointMarker]
    )
    session.Markers.Add(newMarkers)
    logger.info('Point marker "%s" added.', marker_label)


def add_range_marker(
    session: Session,
    marker_start_time: pd.Timestamp,
    marker_end_time: pd.Timestamp,
    marker_label: str,
    marker_group: str = "MARKER",
    marker_description: str = "",
) -> None:
    """Adds a range marker to the session.

    Args:
        session: MESL.SqlRace.Domain.Session to add the range marker to.
        marker_start_time: Start time of the range marker.
        marker_end_time: End time of the range marker.
        marker_label: Label of the marker.
        marker_group: Group name for the marker. Default ``"MARKER"``.
        marker_description: Description text for the marker. Default ``""``.
    """
    marker_start_time = timestamp2long(marker_start_time)
    marker_end_time = timestamp2long(marker_end_time)
    newRangeMarker = Marker(  # .NET objects, so pylint: disable=invalid-name
        int(marker_start_time),
        int(marker_end_time),
        marker_label,
        marker_group,
        marker_description,
    )
    newMarkers = Array[Marker](  # .NET objects, so pylint: disable=invalid-name
        [newRangeMarker]
    )
    session.Markers.Add(newMarkers)
    logger.info(
        'Range marker "%s" added (%s → %s).',
        marker_label,
        marker_start_time,
        marker_end_time,
    )


def add_markers_batch(
    session: Session,
    markers: list[dict],
) -> None:
    """Add multiple markers to the session in a single call.

    Each marker is a dict with the following keys:

    Point marker::

        {"time": pd.Timestamp, "label": str}

    Range marker::

        {"start_time": pd.Timestamp, "end_time": pd.Timestamp,
         "label": str, "group": str (optional), "description": str (optional)}

    Args:
        session: MESL.SqlRace.Domain.Session to add markers to.
        markers: List of marker dicts.
    """
    net_markers = []
    for m in markers:
        if "start_time" in m:
            start = int(timestamp2long(m["start_time"]))
            end = int(timestamp2long(m["end_time"]))
            net_markers.append(
                Marker(
                    start,
                    end,
                    m["label"],
                    m.get("group", "MARKER"),
                    m.get("description", ""),
                )
            )
        else:
            t = int(timestamp2long(m["time"]))
            net_markers.append(Marker(t, m["label"]))

    session.Markers.Add(Array[Marker](net_markers))
    logger.info("%d markers added in batch.", len(net_markers))


def set_session_details(
    session: Session,
    details: dict[str, str],
) -> None:
    """Set session-level metadata (e.g. Driver, Circuit, Vehicle).

    Each key-value pair is written via
    ``session.UpdateUnformattedSessionDetail(key, value)``.  Common keys
    recognised by ATLAS include ``Driver``, ``Circuit``, ``Vehicle``,
    ``Event``, ``Session``, ``Comment``, etc.

    Args:
        session: MESL.SqlRace.Domain.Session to update.
        details: Mapping of detail name → value.

    Example::

        set_session_details(session, {
            "Driver": "Max Verstappen",
            "Circuit": "Silverstone",
            "Vehicle": "MCL60",
            "Event": "British GP 2026",
        })
    """
    for key, value in details.items():
        session.UpdateUnformattedSessionDetail(str(key), str(value))
        logger.info('Session detail "%s" = "%s"', key, value)


# ---------------------------------------------------------------------------
# Synchro (variable-rate) data
# ---------------------------------------------------------------------------

def compute_delta_scale(intervals_ns: np.ndarray) -> int:
    """Compute the GCD of all sample intervals in a packet.

    Args:
        intervals_ns: 1-D int64 array of inter-sample intervals in nanoseconds.

    Returns:
        The greatest common divisor of all intervals (nanoseconds).
    """
    if len(intervals_ns) == 0:
        return 1
    return int(functools.reduce(math.gcd, intervals_ns.astype(np.int64)))


def pack_synchro_packet(
    samples: np.ndarray,
    intervals_ns: np.ndarray,
    delta_scale: int,
) -> bytes:
    """Pack samples and intervals into the interleaved synchro byte format.

    Format per pair: ``[sample (float64 LE, 8 B)] [scaled_interval (uint16 LE, 2 B)]``
    The last sample has no trailing interval.

    Uses a NumPy structured array for zero-copy vectorised packing instead of a
    Python-level ``struct.pack`` loop.

    Args:
        samples: 1-D float64 array of sample values (length N).
        intervals_ns: 1-D int64 array of inter-sample intervals (length N-1).
        delta_scale: The GCD used to scale intervals into uint16.

    Returns:
        Packed byte buffer.

    Raises:
        OverflowError: If any scaled interval exceeds uint16 range (65 535).
    """
    scaled = intervals_ns // delta_scale
    if len(scaled) > 0 and scaled.max() > 65535:
        raise OverflowError(
            f"Scaled interval {scaled.max()} exceeds uint16 range (65535). "
            f"Reduce packet_size or check interval data."
        )

    paired_dtype = np.dtype([("sample", "<f8"), ("interval", "<u2")])
    paired = np.empty(len(intervals_ns), dtype=paired_dtype)
    paired["sample"] = samples[:-1].astype(np.float64)
    paired["interval"] = scaled.astype(np.uint16)

    return paired.tobytes() + samples[-1:].astype(np.float64).tobytes()


def _packet_fits_uint16(intervals_ns: np.ndarray) -> bool:
    """Check whether a set of intervals can be GCD-scaled into uint16."""
    if len(intervals_ns) == 0:
        return True
    gcd = int(functools.reduce(math.gcd, intervals_ns.astype(np.int64)))
    if gcd == 0:
        gcd = 1
    return int(intervals_ns.max()) // gcd <= 65535


def _quantise_intervals(intervals_ns: np.ndarray, resolution_ns: int) -> np.ndarray:
    """Quantise intervals to a fixed resolution to avoid pathological GCDs.

    When timestamps are derived from floating-point arithmetic (e.g.
    ``60e9 / rpm``), the resulting nanosecond intervals can share no
    common factor, giving a GCD of 1 and making every packet overflow
    uint16.  Rounding to microsecond resolution (the default) restores
    a healthy GCD while staying well within the precision of any real
    sensor or crank trigger system.
    """
    quantised = (intervals_ns // resolution_ns) * resolution_ns
    return np.maximum(quantised, resolution_ns)


def split_into_packets(
    samples: np.ndarray,
    timestamps_ns: np.ndarray,
    packet_size: int = 24000,
    quantise_ns: int = 1000,
) -> list[dict]:
    """Split synchro data into sized packets that satisfy the uint16 constraint.

    After the initial size-based split, any packet whose intervals would
    overflow ``uint16`` after GCD scaling is recursively halved until every
    packet is valid.

    Args:
        samples: 1-D float64 sample array (length N).
        timestamps_ns: 1-D int64 timestamp array (length N).
        packet_size: Maximum number of samples per packet (default 24 000,
            yielding ~234 KB payloads for float64 data).  The SQL Race API
            accepts payloads up to approximately 1 MB; above that, data is
            silently discarded on flush.  Safe range: 8 000–100 000.
        quantise_ns: Resolution in nanoseconds to quantise intervals to
            before computing the GCD.  Default 1000 (1 µs).  Set to 1
            to disable quantisation (not recommended for float-derived
            timestamps).

    Returns:
        List of dicts, each with ``samples``, ``intervals_ns``, ``timestamp``.
    """
    intervals_ns = np.diff(timestamps_ns)

    # Quantise intervals to avoid pathological GCD = 1 from float rounding
    if quantise_ns > 1:
        intervals_ns = _quantise_intervals(intervals_ns, quantise_ns)

    packets: list[dict] = []
    n = len(samples)

    raw_chunks: list[tuple[int, int]] = []
    for start in range(0, n, packet_size):
        raw_chunks.append((start, min(start + packet_size, n)))

    stack = list(reversed(raw_chunks))
    while stack:
        start, end = stack.pop()
        if end - start < 2:
            packets.append(
                {
                    "samples": samples[start:end],
                    "intervals_ns": np.array([], dtype=np.int64),
                    "timestamp": int(timestamps_ns[start]),
                }
            )
            continue

        pkt_intervals = intervals_ns[start : end - 1]
        if _packet_fits_uint16(pkt_intervals):
            packets.append(
                {
                    "samples": samples[start:end],
                    "intervals_ns": pkt_intervals,
                    "timestamp": int(timestamps_ns[start]),
                }
            )
        else:
            mid = (start + end) // 2
            stack.append((mid, end))
            stack.append((start, mid))

    return packets


def _create_synchro_config(
    session: Session,
    parameter_name: str,
    app_group: str,
    param_group: str,
    unit: str,
    description: str,
    display_format: str,
    display_limits: tuple | None,
    warning_limits: tuple | None,
    samples: np.ndarray,
) -> int:
    """Create a synchro channel and parameter in the session config.

    Returns:
        The channel ID assigned to this parameter.
    """
    config_id = f"{random.randint(0, 999999):05x}"
    config_mgr = ConfigurationSetManager.CreateConfigurationSetManager()
    config = config_mgr.Create(
        session.ConnectionString, config_id, "Pandlas synchro config"
    )

    group = ParameterGroup(param_group, param_group)
    config.AddParameterGroup(group)

    group_ids = NETList[String]()
    group_ids.Add(group.Identifier)
    app = ApplicationGroup(app_group, app_group, None, group_ids)
    app.SupportsRda = False
    config.AddGroup(app)

    conv_name = f"CONV_{parameter_name}:{app_group}"
    config.AddConversion(
        RationalConversion.CreateSimple1To1Conversion(conv_name, unit, display_format)
    )

    channel_id = session.ReserveNextAvailableRowChannelId() % 2147483647
    channel = Channel(
        channel_id,
        f"{parameter_name}_SynchroChannel",
        0,
        DataType.Double64Bit,
        ChannelDataSourceType.Synchro,
    )
    config.AddChannel(channel)

    param_id = f"{parameter_name}:{app_group}"
    group_idents = NETList[String]()
    group_idents.Add(param_group)

    channel_ids = NETList[UInt32]()
    channel_ids.Add(channel_id)

    disp_min = float(samples.min()) if display_limits is None else display_limits[0]
    disp_max = float(samples.max()) if display_limits is None else display_limits[1]
    warn_min = disp_min if warning_limits is None else warning_limits[0]
    warn_max = disp_max if warning_limits is None else warning_limits[1]

    param = Parameter(
        param_id,
        parameter_name,
        description or f"{parameter_name} description",
        float(disp_max),
        float(disp_min),
        float(warn_max),
        float(warn_min),
        0.0,
        0xFFFF,
        0,
        conv_name,
        group_idents,
        channel_ids,
        app_group,
        display_format,
        unit,
    )
    config.AddParameter(param)

    try:
        config.Commit()
    except ConfigurationSetAlreadyExistsException:
        logger.warning("Config %s already exists.", config_id)

    session.UseLoggingConfigurationSet(config.Identifier)
    logger.info("Synchro config created for %s.", param_id)

    return channel_id


def add_synchro_data(
    session: Session,
    samples: np.ndarray,
    timestamps: Union[np.ndarray, pd.DatetimeIndex],
    parameter_name: str,
    app_group: str = "SynchroApp",
    param_group: str = "SynchroGroup",
    unit: str = "",
    description: str = "",
    display_format: str = "%5.2f",
    display_limits: tuple[float, float] | None = None,
    warning_limits: tuple[float, float] | None = None,
    packet_size: int = 24000,
    show_progress_bar: bool = True,
) -> None:
    """Write variable-rate (synchro) data to an ATLAS session.

    Creates the synchro channel configuration if the parameter doesn't already
    exist, then writes the data in sized packets via
    ``session.AddSynchroChannelData()``.

    Args:
        session: MESL.SqlRace.Domain.Session to write to.
        samples: 1-D float64 array of sample values.
        timestamps: Timestamps for each sample.  Accepts ``pd.DatetimeIndex``
            (converted via ``timestamp2long``) or a raw ``int64`` numpy array
            (nanoseconds in session time-base).
        parameter_name: Name of the synchro parameter.
        app_group: Application group name.
        param_group: Parameter group name.
        unit: Engineering unit string.
        description: Parameter description.
        display_format: Printf-style display format.
        display_limits: ``(min, max)`` display range override.
        warning_limits: ``(min, max)`` warning range override.
        packet_size: Maximum samples per packet (default 8 000).
        show_progress_bar: Show a tqdm progress bar.
    """
    samples = np.asarray(samples, dtype=np.float64)

    if isinstance(timestamps, pd.DatetimeIndex):
        timestamps_ns = timestamp2long(timestamps).astype(np.int64)
    else:
        timestamps_ns = np.asarray(timestamps, dtype=np.int64)

    if len(samples) != len(timestamps_ns):
        raise ValueError(
            f"samples ({len(samples)}) and timestamps ({len(timestamps_ns)}) "
            f"must have the same length."
        )

    param_id = f"{parameter_name}:{app_group}"
    if session.ContainsParameter(param_id):
        parameter = session.GetParameter(param_id)
        channel_id = parameter.Channels[0].Id
        logger.debug("Synchro parameter %s already exists.", param_id)
    else:
        channel_id = _create_synchro_config(
            session,
            parameter_name,
            app_group,
            param_group,
            unit,
            description,
            display_format,
            display_limits,
            warning_limits,
            samples,
        )

    packets = split_into_packets(samples, timestamps_ns, packet_size)
    seq = 0

    for pkt in tqdm(packets, desc="Synchro packets", disable=not show_progress_bar):
        pkt_samples = pkt["samples"]
        pkt_intervals = pkt["intervals_ns"]

        if len(pkt_samples) < 2:
            continue

        delta_scale = compute_delta_scale(pkt_intervals)
        if delta_scale == 0:
            delta_scale = 1

        data_bytes = pack_synchro_packet(pkt_samples, pkt_intervals, delta_scale)

        session.AddSynchroChannelData(
            int(pkt["timestamp"]),
            int(channel_id),
            Byte(seq % 256),
            int(delta_scale),
            bytes(data_bytes),
        )
        seq += 1

    logger.info(
        "Wrote %d synchro packets (%d samples) for %s.",
        len(packets),
        len(samples),
        param_id,
    )


# ---------------------------------------------------------------------------
# Text (enumerated) channels
# ---------------------------------------------------------------------------

def _create_text_config(
    session: Session,
    parameter_name: str,
    app_group: str,
    param_group: str,
    description: str,
    unique_labels: list[str],
    default_label: str,
) -> int:
    """Create a text channel config with TextConversion lookup.

    Uses ``DataType.Signed16Bit`` with ``ChannelDataSourceType.RowData``
    and a ``TextConversion`` that maps integer indices to string labels.
    The format string ``%s`` tells ATLAS to display the converted text.

    Returns:
        The channel ID assigned to this parameter.
    """
    config_id = f"{random.randint(0, 999999):05x}"
    config_mgr = ConfigurationSetManager.CreateConfigurationSetManager()
    config = config_mgr.Create(
        session.ConnectionString, config_id, "Pandlas text channel config"
    )

    group = ParameterGroup(param_group, param_group)
    config.AddParameterGroup(group)

    group_ids = NETList[String]()
    group_ids.Add(group.Identifier)
    app = ApplicationGroup(app_group, app_group, None, group_ids)
    app.SupportsRda = False
    config.AddGroup(app)

    # TextConversion: maps integer indices to string labels
    conv_name = f"TEXT_{parameter_name}:{app_group}"
    numeric_values = Array[Double](
        [float(i) for i in range(len(unique_labels))]
    )
    string_values = Array[String](unique_labels)
    text_conv = TextConversion.Create(
        conv_name,           # name
        "",                  # units
        "%s",                # format string — tells ATLAS to display text
        numeric_values,      # double[] values
        string_values,       # string[] stringValues
        default_label,       # default value
    )
    config.AddConversion(text_conv)

    # Channel — Signed16Bit to match the integer index encoding
    channel_id = session.ReserveNextAvailableRowChannelId() % 2147483647
    channel = Channel(
        channel_id,
        f"{parameter_name}_TextChannel",
        0,
        DataType.Signed16Bit,
        ChannelDataSourceType.RowData,
    )
    config.AddChannel(channel)

    param_id = f"{parameter_name}:{app_group}"
    group_idents = NETList[String]()
    group_idents.Add(param_group)

    channel_ids = NETList[UInt32]()
    channel_ids.Add(channel_id)

    n_labels = len(unique_labels)
    param = Parameter(
        param_id,
        parameter_name,
        description or f"{parameter_name} text channel",
        float(n_labels - 1),
        0.0,
        float(n_labels - 1),
        0.0,
        0.0,
        0xFFFF,
        0,
        conv_name,
        group_idents,
        channel_ids,
        app_group,
        "%s",
        "",
    )
    config.AddParameter(param)

    try:
        config.Commit()
    except ConfigurationSetAlreadyExistsException:
        logger.warning("Config %s already exists.", config_id)

    session.UseLoggingConfigurationSet(config.Identifier)
    logger.info("Text config created for %s (%d labels).", param_id, n_labels)

    return channel_id


def add_text_channel(
    session: Session,
    parameter_name: str,
    values: Union[list, np.ndarray, pd.Series, pd.Categorical],
    timestamps: Union[pd.DatetimeIndex, np.ndarray],
    application_group: str = "TextApp",
    param_group: str = "TextGroup",
    description: str = "",
    default_label: str = "Unknown",
) -> None:
    """Write an enumerated text channel to an ATLAS session.

    Builds a ``TextConversion`` lookup table from the unique string values,
    encodes each value as a ``Signed16Bit`` integer index, and writes the data
    as a normal row-data channel.  ATLAS displays the string labels via the
    ``TextConversion``.

    Args:
        session: MESL.SqlRace.Domain.Session to write to.
        parameter_name: Name of the text parameter.
        values: Sequence of string values (one per timestamp).
        timestamps: ``pd.DatetimeIndex`` or int64 nanosecond array.
        application_group: Application group name.
        param_group: Parameter group name.
        description: Parameter description.
        default_label: Default label for values not in the lookup table.
    """
    # Convert values to list of strings
    if isinstance(values, pd.Series):
        str_values = values.astype(str).tolist()
    elif isinstance(values, np.ndarray):
        str_values = [str(v) for v in values]
    else:
        str_values = [str(v) for v in values]

    # Convert timestamps
    if isinstance(timestamps, pd.DatetimeIndex):
        timestamps_ns = timestamp2long(timestamps).astype(np.int64)
    else:
        timestamps_ns = np.asarray(timestamps, dtype=np.int64)

    if len(str_values) != len(timestamps_ns):
        raise ValueError(
            f"values ({len(str_values)}) and timestamps ({len(timestamps_ns)}) "
            f"must have the same length."
        )

    # Build lookup table: unique string → integer index
    unique_labels = list(dict.fromkeys(str_values))
    label_to_idx = {label: i for i, label in enumerate(unique_labels)}

    # Encode as int16 (matches Signed16Bit channel)
    encoded = np.array(
        [label_to_idx[v] for v in str_values], dtype=np.int16
    )

    # Create config if parameter doesn't exist
    param_id = f"{parameter_name}:{application_group}"
    if not session.ContainsParameter(param_id):
        _create_text_config(
            session,
            parameter_name,
            application_group,
            param_group,
            description,
            unique_labels,
            default_label,
        )

    # Get channel ID
    parameter = session.GetParameter(param_id)
    channel_id = parameter.Channels[0].Id

    # Write data — 2 bytes per int16 sample
    ts_array = Array[Int64](timestamps_ns.tolist())
    data_bytes = bytes(encoded.tobytes())
    session.AddRowData(int(channel_id), ts_array, data_bytes, 2, False)

    logger.info(
        "Wrote %d text samples for %s (%d unique labels).",
        len(str_values),
        param_id,
        len(unique_labels),
    )


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

_PRIORITY_MAP = {
    "low": EventPriorityType.Low,
    "medium": EventPriorityType.Medium,
    "high": EventPriorityType.High,
}


def add_events(
    session: Session,
    df: pd.DataFrame,
    timestamp_column: str = "timestamp",
    status_column: str = None,
    application_group: str = "Events",
    description: str = "Events",
    priority: str = "medium",
    conversion_format: str = "%5.2f",
    event_definition_id: int = None,
) -> int:
    """Write a DataFrame of events to an ATLAS session.

    Each row in *df* becomes one event. Every numeric column (excluding
    *timestamp_column* and *status_column*) is attached as an event data value.

    Args:
        session: An open ATLAS session in write mode.
        df: DataFrame where each row is an event. Must contain a
            *timestamp_column* of ``pd.Timestamp`` (or int64 nanoseconds)
            and one or more numeric value columns.
        timestamp_column: Name of the column holding event times.
        status_column: Optional column with a string label per event
            (e.g. ``"LOCKUP-001"``). Shown in the ATLAS **Status** column.
            When *None*, an auto-incrementing label like
            ``"BrakeApp_001"`` is generated.
        application_group: ATLAS application group name.
        description: Human-readable description shown in ATLAS.
        priority: ``"low"``, ``"medium"`` (default), or ``"high"``.
        conversion_format: C-style format string for numeric display.
        event_definition_id: Optional fixed ID (0–0xFFFF). Auto-generated
            if *None*.

    Returns:
        Number of events written.

    Example::

        import pandas as pd
        from pandlas import SQLRaceDBConnection, add_events

        events = pd.DataFrame({
            "timestamp": pd.date_range("2026-04-01 14:00", periods=5, freq="30s"),
            "Status":         ["LK-001", "LK-002", "LK-003", "LK-004", "LK-005"],
            "LockupPressure": [32.1, 28.5, 35.0, 30.2, 29.8],
            "WheelSlip":      [0.12, 0.08, 0.15, 0.10, 0.09],
        })

        with SQLRaceDBConnection(..., mode="w") as session:
            # ... write row data first ...
            add_events(session, events, status_column="Status",
                       description="Lockup Events",
                       priority="high", application_group="BrakeApp")
    """
    if timestamp_column not in df.columns:
        raise ValueError(f"Column '{timestamp_column}' not found in DataFrame.")

    # Resolve priority
    net_priority = _PRIORITY_MAP.get(priority.lower())
    if net_priority is None:
        raise ValueError(
            f"Invalid priority '{priority}'. Use 'low', 'medium', or 'high'."
        )

    # Columns excluded from numeric data
    exclude = {timestamp_column}
    if status_column is not None:
        if status_column not in df.columns:
            raise ValueError(f"Column '{status_column}' not found in DataFrame.")
        exclude.add(status_column)

    # Separate timestamps from value columns
    value_cols = [c for c in df.columns if c not in exclude]
    if not value_cols:
        raise ValueError(
            "DataFrame must have at least one numeric column besides the "
            "timestamp and status columns."
        )

    # Convert timestamps to nanosecond int64
    ts_series = df[timestamp_column]
    if pd.api.types.is_datetime64_any_dtype(ts_series):
        timestamps_ns = timestamp2long(pd.DatetimeIndex(ts_series)).astype(np.int64)
    else:
        timestamps_ns = np.asarray(ts_series, dtype=np.int64)

    # Auto-generate event definition ID
    if event_definition_id is None:
        event_definition_id = random.randint(0x0001, 0xFFFE)

    # Create event configuration
    conn_str = session.ConnectionString
    config_name = f"EventCfg_{application_group}_{event_definition_id}"
    config = ConfigurationSetManager.CreateConfigurationSetManager() \
        .Create(conn_str, config_name, f"{description} config")

    # One rational conversion per value column
    conv_names = []
    for col in value_cols:
        conv_name = f"conv_{col}_{event_definition_id}"
        config.AddConversion(
            RationalConversion.CreateSimple1To1Conversion(
                conv_name, "", conversion_format,
            )
        )
        conv_names.append(conv_name)

    # Create event definition
    conv_array = Array[String](conv_names)
    event_def = EventDefinition(
        event_definition_id,
        description,
        net_priority,
        conv_array,
        application_group,
    )
    config.AddEventDefinition(event_def)

    try:
        config.Commit()
    except ConfigurationSetAlreadyExistsException:
        logger.debug("Event config %s already exists, reusing.", config_name)

    session.UseLoggingConfigurationSet(config.Identifier, UInt32(100))

    # Write each row as an event
    n_written = 0
    for i in range(len(df)):
        ts = int(timestamps_ns[i])
        raw_values = [float(df[col].iloc[i]) for col in value_cols]
        raw_array = Array[Double](raw_values)

        if status_column is not None:
            status_text = str(df[status_column].iloc[i])
        else:
            status_text = f"{application_group}_{i + 1:03d}"

        session.Events.AddEventData(
            event_definition_id, application_group, ts, raw_array,
            False, status_text,
        )
        n_written += 1

    logger.info(
        "Wrote %d events (%s) with %d value(s) each to '%s'.",
        n_written,
        description,
        len(value_cols),
        application_group,
    )
    return n_written

