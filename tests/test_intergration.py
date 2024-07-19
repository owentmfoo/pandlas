"""Integration / Full system test going all the way from Pandlas to ATLAS"""
import pytest
import pandlas.SqlRace as sr
import numpy as np
import pandas as pd
from pandlas.utils import timestamp2long


@pytest.mark.atlaslicensed()
def test_read_write_sqlite(tmp_path):
    """Write to SQLite and then read the session back."""
    SQLITE_DB_DIR = rf'{tmp_path}\temp.ssndb'
    session_identifier = "TestSession"

    start = np.datetime64("now")

    df = pd.DataFrame()
    df.index = pd.date_range(start, periods=1000, freq='s')
    df.loc[:, "Param 1"] = np.sin(np.linspace(0, 10 * np.pi, num=1000))
    with sr.SQLiteConnection(SQLITE_DB_DIR, session_identifier, mode='w') as session:
        df.atlas.to_atlas_session(session,show_progress_bar=False)
        key = session.Key.ToString()

    with sr.SQLiteConnection(SQLITE_DB_DIR, session_key=key, mode='r') as session:
        assert session_identifier == session.Identifier
        samples, timstamp = sr.get_samples(session, "Param 1:MyApp")
        np.isclose(df["Param 1"].to_numpy(),samples)
        np.isclose(timestamp2long(df["Param 1"].index),timstamp)


@pytest.mark.atlaslicensed()
def test_read_write_sqldb(tmp_path):
    """Write to SQLite and then read the session back."""
    session_identifier = "TestSession"
    data_source = r'MCLA-5JRZTQ3\LOCAL'
    database = 'SQLRACE01'
    start = np.datetime64("now")

    df = pd.DataFrame()
    df.index = pd.date_range(start, periods=1000, freq='s')
    df.loc[:, "Param 1"] = np.sin(np.linspace(0, 10 * np.pi, num=1000))
    with sr.SQLRaceDBConnection(data_source, database, session_identifier, mode='w') as session:
        df.atlas.to_atlas_session(session,show_progress_bar=False)
        key = session.Key.ToString()

    with sr.SQLRaceDBConnection(data_source, database, session_key=key, mode='r') as session:
        assert session_identifier == session.Identifier
        samples, timstamp = sr.get_samples(session, "Param 1:MyApp")
        np.isclose(df["Param 1"].to_numpy(),samples)
        np.isclose(timestamp2long(df["Param 1"].index),timstamp)