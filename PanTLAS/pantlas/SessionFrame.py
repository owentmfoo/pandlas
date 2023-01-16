"""As a subclass for dataframe"""
import datetime
import os
from typing import Union, List

import numpy as np
import pandas as pd
import clr
import logging

logging.basicConfig(level=logging.DEBUG)

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

if not os.path.isfile(ssn2splitter_dll_path):
    raise Exception(f"Couldn't find SSN2 Splitter DLL at {ssn2splitter_dll_path}.")

clr.AddReference(ssn2splitter_dll_path)

from System.Collections.Generic import *
from System.Collections.ObjectModel import *
from System import *

from MAT.OCS.Core import *
from MESL.SqlRace.Domain import *
from MESL.SqlRace.Enumerators import *
from MESL.SqlRace.Domain.Infrastructure.DataPipeline import *
from MAT.SqlRace.Ssn2Splitter import Ssn2SessionExporter


class SessionFrame(pd.DataFrame):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ParameterGroupIdentifier = "SessionFrame"
        self.ApplicationGroupName = "MyApp"
        # TODO: check if index is datatime index or timedelta index
        # TODO: check if a dataframe is passed in or not

    def to_ssn2(self, session):
        """Add the contents of the frame to the ATLAS session."""
        config = session.CreateConfiguration()

        # Add param group
        # TODO: check if it is present
        ParameterGroupIdentifier = self.ParameterGroupIdentifier
        group1 = ParameterGroup(ParameterGroupIdentifier, ParameterGroupIdentifier)
        config.AddParameterGroup(group1)

        # Add app group
        ApplicationId = 998
        ApplicationGroupName = self.ApplicationGroupName
        parameterGroupIds = List[String]()
        parameterGroupIds.Add(group1.Identifier)
        applicationGroup1 = ApplicationGroup(
            ApplicationGroupName,
            ApplicationGroupName,
            ApplicationId,
            parameterGroupIds)
        applicationGroup1.SupportsRda = False
        config.AddGroup(applicationGroup1)

        ParameterGroupIdentifier = self.ParameterGroupIdentifier
        ApplicationGroupName = self.ApplicationGroupName

        # Create channel conversion function
        ConversionFunctionName = "Simple1To1"
        config.AddConversion(RationalConversion.CreateSimple1To1Conversion(ConversionFunctionName, "", "%5.2f"))

        # Add param channel
        MyParamChannelId = 999998
        myParamFrequency = Frequency(2, FrequencyUnit.Hz)
        myParameterChannel = Channel(
            MyParamChannelId,
            "MyParamChannel",
            FrequencyExtensions.ToInterval(myParamFrequency),
            DataType.FloatingPoint32Bit,
            ChannelDataSourceType.RowData)
        config.AddChannel(myParameterChannel)

        #  Add param
        ParameterName = "MyParam"
        parameterIdentifier = f"{ParameterName}:{ApplicationGroupName}"
        parameterGroupIdentifiers = List[String]()
        parameterGroupIdentifiers.Add(ParameterGroupIdentifier)
        myParameter = Parameter(
            parameterIdentifier,
            ParameterName,
            ParameterName + "Description",
            1.0,
            -1.0,
            1.0,
            -1.0,
            0.0,
            0xFFFF,
            0,
            ConversionFunctionName,
            parameterGroupIdentifiers,
            myParameterChannel.Id,
            ApplicationGroupName)
        config.AddParameter(myParameter)

        config.Commit()

        # Add some random data
        samplecount = int((session.EndTime - session.StartTime) / FrequencyExtensions.ToInterval(myParamFrequency))
        # data = np.random.randint(0, 2, samplecount)
        data = np.random.rand(samplecount)
        # data = np.sin(np.linspace(0,100,samplecount))
        netarray = Array[Double](data.astype(float).tolist())

        timestamps = pd.date_range(DateTime.Today.AddMilliseconds(session.StartTime / 1e6).ToString(),
                                   DateTime.Today.AddMilliseconds(session.EndTime / 1e6 + 600e3).ToString(),
                                   samplecount)

        for timestamp, datapoint in zip(timestamps, data.astype(float).tolist()):
            # timestamp = int(sessionDate.TimeOfDay.TotalMilliseconds * 1e6 + FrequencyExtensions.ToInterval(
            # myParamFrequency) * i)
            timestamp = ((timestamp.hour * 3600 + timestamp.minute * 60 + timestamp.second) * 1e9 + timestamp.microsecond * 1e3 +
                         timestamp.nanosecond)
            channelIds = List[UInt32]()
            channelIds.Add(MyParamChannelId)
            session.AddRowData(Int64(int(timestamp)), channelIds, BitConverter.GetBytes(datapoint))

        logging.info('Data added.')
        print(session.get_CurrentConfigurationSets().get_Count())


if __name__ == '__main__':

    #  Initialise SQLRace
    logging.info('Initialising SQLRace API')
    if not Core.IsInitialized:
        Core.LicenceProgramName = "SQLRace"
        Core.Initialize()
    logging.info('SQLRace API Initialised')

    litedbdir = r'c:\temp\PanTLAS\temp.ssndb'
    pathToFile = r'c:\ssn2\test1.ssn2'
    pathToFile = os.path.abspath(pathToFile)
    output_dir = os.path.dirname(pathToFile)

    #  Create new session
    connectionString = rf"DbEngine=SQLite;Data Source={litedbdir};"
    sessionManager = SessionManager.CreateSessionManager()
    sessionKey = SessionKey.NewKey()
    sessionIdentifier = os.path.basename(pathToFile).strip(".ssn2")
    sessionDate = DateTime.Now
    eventType = 'SessionFrame'
    clientSession = sessionManager.CreateSession(connectionString, sessionKey, sessionIdentifier,
                                                 sessionDate, eventType)
    session = clientSession.Session
    logging.info('Session created')
    #  Add 1 lap
    for i in range(4):
        newlap = Lap(int(sessionDate.TimeOfDay.TotalMilliseconds * 1e6 + 60e9 * (i)), i, Byte(0), f"Lap{i + 1}",
                     True)
        session.Laps.Add(newlap)

    sf = SessionFrame(pd.DataFrame(np.random.random([5, 2])))
    sf.to_ssn2(session)

    clientSession.Close()
    exporter = Ssn2SessionExporter()
    exporter.Export(sessionKey.ToString(), litedbdir, output_dir)
