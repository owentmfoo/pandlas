"""Pandlas

An example package demonstrating how to use the SQLRace API. This package is not maintained nor officially supported.
"""
import os
import clr  # Pythonnet
import numpy as np

from pandlas.session_frame import SessionFrame
from pandlas.SqlRace import SQLRaceDBConnection,SQLiteConnection,Ssn2Session