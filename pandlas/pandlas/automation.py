"""Python wrapper for a selection of ALTAS Automation API calls

See Also
    Automation API Documentation: https://mat-docs.github.io/Atlas.DisplayAPI.Documentation/articles/automation.html
"""
from typing import List

import clr
import subprocess
import threading
import os

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

from MAT.Atlas.Automation.Client.Services import ApplicationServiceClient, WorkbookServiceClient, SetServiceClient
from MAT.Atlas.Automation.Api.Models import *
from System.IO import Path
from System import AppDomain


def open_atlas(app):
    # Open ATLAS10 from default installation location
    subprocess.Popen(r"C:\Program Files\McLaren Applied Technologies\ATLAS 10\MAT.ATLAS.Host.exe")
    print("Waiting for ATLAS to open")

    import time
    time.sleep(10)
    # set up multiprocessing.Lock to be used to block until client is connected.
    connect = threading.Lock()
    connect.acquire()

    def client_connected(client_name):
        # event handler for OnClientConnected
        print('\nATLAS Client connected.')
        print(f'ATLAS version: {app.GetVersion()}')
        connect.release()

    app.OnClientConnected += client_connected
    app.Connect(Path.GetFileNameWithoutExtension(AppDomain.CurrentDomain.FriendlyName))
    # Wait until client is connected. The next line cannot run until connect is released by the handeler function.
    connect.acquire()
    connect.release()


def load_session(app, setid, keys: List[str], connection_string: List[str]):
    load = threading.Lock()
    load.acquire()

    def session_loaded(session_loaded:SessionLoaded):
        # event handeler for OnSessionLoaded
        print('Session loaded.')
        load.release()

    app.OnSessionLoaded += session_loaded
    # Loading session from SQL Race into specified set, using session keys found in SQLRace API and connection string to SQL Race
    print('\nLoading session...')
    app.LoadSqlRaceSessions(setid, keys, connection_string)
    # wait for session to load
    load.acquire()
    load.release()

