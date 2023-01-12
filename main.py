import os
import clr  # Pythonnet
import numpy as np

# The path to the main SQL Race DLL. This is the default location when installed with Atlas 10
sql_race_dll_path = r"C:\Program Files\McLaren Applied Technologies\ATLAS 10\MESL.SqlRace.Domain.dll"
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


clr.AddReference(r"C:\Program Files\McLaren Applied Technologies\ATLAS 10\MAT.SqlRace.Ssn2Splitter.dll")

from System.Collections.Generic import *
from System.Collections.ObjectModel import *
from System import *

from MAT.OCS.Core import *
from MESL.SqlRace.Domain import *
from MESL.SqlRace.Enumerators import *
from MESL.SqlRace.Domain.Infrastructure.DataPipeline import *
from MAT.SqlRace.Ssn2Splitter import Ssn2SessionExporter

#  Initialise SQLRace
Core.LicenceProgramName = "SQLRace"
Core.Initialize()

#  Create new session
pathToSession = r"c:\ssn2\test01.ssndb"
connectionString = rf"DbEngine=SQLite;Data Source={pathToSession};"
sessionManager = SessionManager.CreateSessionManager()
sessionKey = SessionKey.NewKey()
sessionIdentifier = 'TestSession'
sessionDate = DateTime.Now
eventType = 'Test'

clientSession = sessionManager.CreateSession(connectionString, sessionKey, sessionIdentifier,
                                             sessionDate, eventType)
session = clientSession.Session

#  Add 5 lap
for i in range(1, 6):
    newlap = Lap(int(sessionDate.TimeOfDay.TotalMilliseconds * 1e6 + 60e9 * (i)), i, Byte(0), f"Lap{i}", True)
    session.Laps.InsertItem(0, newlap)

#  Create parameter groups
ParameterName = "MyParam"
ParameterGroupIdentifier = "MyParamGroup"
ApplicationGroupName = "MyApp"
ApplicationId = 998
MyParamChannelId = 999998
ConversionFunctionName = "CONV_MyParam:MyApp"
parameterIdentifier = f"{ParameterName}:{ApplicationGroupName}"

config = session.CreateConfiguration()

# Add param group
group1 = ParameterGroup(ParameterGroupIdentifier, "pg1_description")
config.AddParameterGroup(group1)

# Add app group
parameterGroupIds = List[String]()
parameterGroupIds.Add(group1.Identifier)
applicationGroup1 = ApplicationGroup(
    ApplicationGroupName,
    ApplicationGroupName,
    ApplicationId,
    parameterGroupIds)
applicationGroup1.SupportsRda = False
config.AddGroup(applicationGroup1)

# Create channel conversion function
config.AddConversion(RationalConversion.CreateSimple1To1Conversion(ConversionFunctionName, "kph", "%5.2f"))

# Add param channel
myParamFrequency = Frequency(2, FrequencyUnit.Hz)
myParameterChannel = Channel(
    MyParamChannelId,
    "MyParamChannel",
    FrequencyExtensions.ToInterval(myParamFrequency),
    DataType.FloatingPoint32Bit,
    ChannelDataSourceType.Periodic)
config.AddChannel(myParameterChannel)

#  Add param
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
for i, datum in enumerate(data):
    session.AddChannelData(MyParamChannelId,
                           int(sessionDate.TimeOfDay.TotalMilliseconds * 1e6 + FrequencyExtensions.ToInterval(
                               myParamFrequency) * i), 1,
                           BitConverter.GetBytes(netarray[i]))

clientSession.Close()

exporter = Ssn2SessionExporter()
exporter.Export(sessionKey.ToString(), pathToSession, 'C:/ssn2/')
