"""Class file for SessionFrame

SessionFrame is an extension of pandas DataFrame under the namespace 'atlas', and it has additional methods to interact
with the ATLAS APIs through pythonnet.

ATLAS APIs are build on the .NET Framework (and will be moved to .NET Core in the near future).

See Also:
    SQLRace API Documenetation: https://mat-docs.github.io/Atlas.SQLRaceAPI.Documentation/api/index.html
    Automation API Documentation: https://mat-docs.github.io/Atlas.DisplayAPI.Documentation/articles/automation.html
    API Examples Github: https://github.com/mat-docs

"""
import datetime
import os
import random
import warnings
import struct
from typing import Union, List
import numpy.typing as npt
import numpy as np
import pandas as pd
import clr
import logging
from tqdm import tqdm
from pandlas.utils import timestamp2long
logger = logging.getLogger(__name__)

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


@pd.api.extensions.register_dataframe_accessor("atlas")
class SessionFrame:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj
        self.ParameterGroupIdentifier = "SessionFrame"
        self.ApplicationGroupName = "MyApp"
        self.paramchannelID = dict()
        if not isinstance(self._obj.index, pd.DatetimeIndex):
            warnings.warn("DataFrame index is not pd.DatetimeIndex, attempting to parse index to DatetimeIndex...")
            try:
                self._obj.index = pd.to_datetime(self._obj.index)
                warnings.warn("parse success.")
            except:
                warnings.warn("parse failed.")

    def to_atlas_session(self, session: Session, show_progress_bar: bool = True):
        """Add the contents of the DataFrame to the ATLAS session.

        The index of the DataFrame must be a DatetimeIndex, or else a AttributeError will be raised.
        All the data should be float or can be converted to float.
        A row channel will be created for each column and the parameter will be named using the column name.

        Args:
            session: MESL.SqlRace.Domain.Session to the data to
            show_progress_bar: Show progress bar when creating config and adding data.
        Raises:
             AttributeError: The index is not a pd.DatetimeIndex.
        """

        if not isinstance(self._obj.index, pd.DatetimeIndex):
            raise TypeError("SessionFrame index is not pd.DatetimeIndex, unable to export to ssn2")

        # add a lap at the start of the session, if a column named Lap is present then it will add it
        # TODO: add the rest of the laps
        timestamp = self._obj.index[0]
        timestamp64 = timestamp2long(timestamp)
        try:
            lap = self._obj.loc[timestamp].Lap
        except:
            lap = 1
        newlap = Lap(int(timestamp64), int(lap), Byte(0), f"Lap {lap}",
                     True)
        # TODO: what to do when you add to an existing session.
        if session.LapCollection.Count == 0:
            session.LapCollection.Add(newlap)

        # check if there is config for it already
        need_new_config = False
        for param_name in self._obj.columns:
            param_identifier = f'{param_name}:{self.ApplicationGroupName}'
            if not session.ContainsParameter(param_identifier):
                need_new_config = True

        if need_new_config:
            config_identifier = f"{random.randint(0, 999999):05x}"
            config_decription = "SessionFrame generated config"
            configSetManager = ConfigurationSetManager.CreateConfigurationSetManager()
            config = configSetManager.Create(session.ConnectionString, config_identifier, config_decription)

            # Add param group
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

            # obtain the data
            for param_name in tqdm(self._obj.columns, desc="Creating channels", disable=not show_progress_bar):
                if session.ContainsParameter(param_identifier):
                    continue
                timestamps = self._obj.index
                data = self._obj.loc[:, param_name].to_numpy()
                dispmax = data.max()
                dispmin = data.min()
                warnmax = dispmax
                warnmin = dispmin

                # Add param channel
                MyParamChannelId = session.ReserveNextAvailableRowChannelId() % 2147483647
                # TODO: this is a stupid workaround because it takes UInt32 but it cast it to Int32 internally...
                self._add_channel(config, MyParamChannelId, param_name)

                #  Add param
                self._add_param(config, ApplicationGroupName, ConversionFunctionName,
                                ParameterGroupIdentifier, dispmax, dispmin, param_name, warnmax, warnmin)

            try:
                config.Commit()
            except:
                logging.warning(f"cannot commit config {config.Identifier}, config already exist.")
            session.UseLoggingConfigurationSet(config.Identifier)

        # Obtain the channel Id for the existing parameters
        for param_name in self._obj.columns:
            if not session.ContainsParameter(param_identifier):
                continue
            param_identifier = f'{param_name}:{self.ApplicationGroupName}'
            Parameter = session.GetParameter(param_identifier)
            if Parameter.Channels.Count != 1:
                logger.warning('Parameter %s contains more than 1 channel', param_identifier)
            self.paramchannelID[param_name] = Parameter.Channels[0].Id

        # write it to the session
        for param_name in tqdm(self._obj.columns, desc="Adding data", disable=not show_progress_bar):
            timestamps = self._obj.index
            data = self._obj.loc[:, param_name].to_numpy()
            MyParamChannelId = self.paramchannelID[param_name]
            self.add_data(session, MyParamChannelId, data, timestamps)

        logging.debug('Data for {}:{} added.'.format(self.ParameterGroupIdentifier, self.ApplicationGroupName))

    def _add_param(self, config: ConfigurationSet, ApplicationGroupName: str, ConversionFunctionName: str,
                   ParameterGroupIdentifier: str, dispmax: float, dispmin: float, paramName: str, warnmax: float,
                   warnmin: float):
        """ Adds a parameter to the ConfigurationSet.

        Args:
            config: ConfigurationSet to add to.
            ApplicationGroupName: Name of the ApplicationGroup to be under
            ConversionFunctionName: Name of the conversion factor to apply.
            ParameterGroupIdentifier: ID of the ParameterGroup.
            dispmax: Display maximum.
            dispmin: Display minimum.
            paramName: Parameter name.
            warnmax: Warning maximum.
            warnmin: Warning minimum.

        """
        # TODO: guard again NaNs
        ParameterName = paramName
        MyParamChannelId = self.paramchannelID[paramName]
        parameterIdentifier = f"{ParameterName}:{ApplicationGroupName}"
        parameterGroupIdentifiers = List[String]()
        parameterGroupIdentifiers.Add(ParameterGroupIdentifier)
        myParameter = Parameter(
            parameterIdentifier,
            ParameterName,
            ParameterName + "Description",
            float(dispmax),
            float(dispmin),
            float(warnmax),
            float(warnmin),
            0.0,
            0xFFFF,
            0,
            ConversionFunctionName,
            parameterGroupIdentifiers,
            MyParamChannelId,
            ApplicationGroupName)
        config.AddParameter(myParameter)

    def _add_channel(self, config: ConfigurationSet, channel_id: int, paramName: str):
        """Adds a row channel to the config.

        Args:
            config: ConfigurationSet to add to.
            channel_id: ID of the channel.
            paramName: Name of the parameter.
        """
        self.paramchannelID[paramName] = channel_id
        myParameterChannel = Channel(
            channel_id,
            "MyParamChannel",
            0,
            DataType.FloatingPoint32Bit,
            ChannelDataSourceType.RowData)
        config.AddChannel(myParameterChannel)

    def add_data(self, session: Session, channel_id: float,
                 data: np.ndarray, timestamps: Union[pd.DatetimeIndex, npt.NDArray[np.datetime64]]):
        """Adds data to a channel.

        Args:
            session: Session to add data to.
            channel_id: ID of the channel.
            data: numpy array of float or float equivalents
            timestamps: timestamps for the datapoints
        """
        # TODO: add in guard against invalid datatypes
        if not isinstance(timestamps, (pd.DatetimeIndex, npt.NDArray[np.datetime64])):
            raise TypeError("timestamps should be pd.DateTimeIndex, or numpy array of np.datetime64.")
        timestamps = timestamp2long(timestamps)

        channelIds = List[UInt32]()
        channelIds.Add(channel_id)

        # databytes = data.astype(np.float32).tobytes()
        databytes = bytearray(len(data)*4)
        for i,value in enumerate(data):
            new_bytes = struct.pack('f', value)
            padding_length = 4 - len(new_bytes)
            databytes[i*4:i*4+len(new_bytes)] = new_bytes

        timestamps_array = Array[Int64](len(timestamps))
        for i, timestamp in enumerate(timestamps):
            timestamps_array[i] = Int64(int(timestamp))

        session.AddRowData(channel_id, timestamps_array, databytes, 4, False)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    from pandlas.SqlRace import initialise_sqlrace

    initialise_sqlrace()

    litedbdir = r'c:\temp\pandlas\temp.ssndb'
    pathToFile = r'c:\ssn2\test1.ssn2'
    pathToFile = os.path.abspath(pathToFile)
    output_dir = os.path.dirname(pathToFile)

    create_new_session = True
    load_existing = False

    if create_new_session:
        #  Create new session`
        connectionString = rf"DbEngine=SQLite;Data Source={litedbdir};"
        sessionManager = SessionManager.CreateSessionManager()
        sessionKey = SessionKey.NewKey()
        sessionIdentifier = os.path.basename(pathToFile).strip(".ssn2")
        sessionDate = DateTime.Now
        eventType = 'Session'
        clientSession = sessionManager.CreateSession(connectionString, sessionKey, sessionIdentifier,
                                                     sessionDate, eventType)
        session = clientSession.Session
        logging.info('Session created')

        # #  Add 1 lap
        # for i in range(4):
        #     newlap = Lap(int(sessionDate.TimeOfDay.TotalMilliseconds * 1e6 + 60e9 * (i)), i, Byte(0), f"Lap{i + 1}",
        #                  True)
        #     session.Laps.Add(newlap)

        # Add some random data
        # samplecount = int((session.EndTime - session.StartTime) / 1e9) * 10  # 10Hz sample rate
        # timestamps = pd.date_range(DateTime.Today.AddMilliseconds(session.StartTime / 1e6).ToString(),
        #                            DateTime.Today.AddMilliseconds(session.EndTime / 1e6).ToString(),
        #                            samplecount)
        #
        # df = pd.DataFrame(index=timestamps)
        # sf = SessionFrame(df)
        # sf.loc[:, 'Sine'] = np.sin(np.linspace(0, 100, samplecount))
        # sf.loc[:, 'Random'] = np.random.rand(samplecount)
        # sf.loc[:, 'Random int'] = np.random.randint(0, 2, samplecount)

        df = pd.read_csv(
            "https://data.nationalgrideso.com/backend/dataset/88313ae5-94e4-4ddc-a790-593554d8c6b9/resource/f93d1835-75bc-43e5-84ad-12472b180a98/download/df_fuel_ckan.csv")
        df.set_index('DATETIME', inplace=True)
        df.index = pd.to_datetime(df.index)
        # sf = SessionFrame(df.loc["2023-01-21":"2023-01-22"])
        df = df.loc["2023-01-21":"2023-01-22"]
        df.atlas.to_atlas_session(session)

        clientSession.Close()
        # exporter = Ssn2SessionExporter()
        # exporter.Export(sess`ionKey.ToString(), litedbdir, output_dir)

    if load_existing:
        keys = ["21a0153a-a414-41bc-9ca8-c4f39921d4f0"]
        connection_string = r"DBEngine=SQLite;Data Source=D:\SSN2\Energy.ssn2"
        connection_strings = [connection_string]
    elif create_new_session:  # load the newly created session if it was previously created
        keys = [sessionKey.ToString()]
        connectionStrings = [connectionString]
    # Building connection strings list for LoadingSQLRaceSession

    from MAT.Atlas.Automation.Client.Services import ApplicationServiceClient, WorkbookServiceClient, SetServiceClient
    from automation import open_atlas, load_session

    app = ApplicationServiceClient()
    open_atlas(app)

    workbookServiceClient = WorkbookServiceClient()

    workbookServiceClient.ReplaceWorkbook(r"D:\2023R1 Demo\Energy.wbkx")
    import time

    time.sleep(5)
    setsList = workbookServiceClient.GetSets()
    load_session(app, setsList[0].Id, keys, connection_strings)
