"""Pandlas

An example package demonstrating how to use the SQLRace API. This package is not
maintained nor officially supported.
"""

from importlib.metadata import version

__version__ = version(__package__)

from pandlas.session_frame import SessionFrame
from pandlas.SqlRace import (
    SQLiteConnection,
    Ssn2Session,
    SQLRaceDBConnection,
    get_samples,
    add_lap,
    update_lap,
    add_point_marker,
    add_range_marker,
    add_markers_batch,
    set_session_details,
    add_synchro_data,
    add_text_channel,
    add_events,
)
