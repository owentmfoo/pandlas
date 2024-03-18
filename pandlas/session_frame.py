# pylint: disable=undefined-variable
"""Class file for SessionFrame

SessionFrame is an extension of pandas DataFrame under the namespace 'atlas', and it has
additional methods to interact with the ATLAS APIs through pythonnet.

ATLAS APIs are build on the .NET Core.

See Also:
    SQLRace API Documenetation:
        https://mat-docs.github.io/Atlas.SQLRaceAPI.Documentation/api/index.html
    Automation API Documentation:
        https://mat-docs.github.io/Atlas.DisplayAPI.Documentation/articles/automation.html
    API Examples Github:
        https://github.com/mat-docs

"""
import os
import random
import warnings
import struct
from concurrent.futures import wait, ThreadPoolExecutor
from typing import Union
import logging
import numpy.typing as npt
import numpy as np
import pandas as pd
import clr
from tqdm import tqdm
from pandlas.utils import timestamp2long

logger = logging.getLogger(__name__)

A10_INSTALL_PATH = r"C:\Program Files\McLaren Applied Technologies\ATLAS 10"
SQL_RACE_DLL_PATH = rf"{A10_INSTALL_PATH}\MESL.SqlRace.Domain.dll"
SSN2SPLITER_DLL_PATH = rf"{A10_INSTALL_PATH}\MAT.SqlRace.Ssn2Splitter.dll"

# Configure Pythonnet and reference the required assemblies for dotnet and SQL Race
clr.AddReference("System.Collections")  # pylint: disable=no-member
clr.AddReference("System.Core")  # pylint: disable=no-member
clr.AddReference("System.IO")  # pylint: disable=no-member

if not os.path.isfile(SQL_RACE_DLL_PATH):
    raise FileNotFoundError(
        f"Couldn't find SQL Race DLL at {SQL_RACE_DLL_PATH} please check that Atlas 10 "
        f"is installed."
    )

clr.AddReference(SQL_RACE_DLL_PATH)  # pylint: disable=no-member

if not os.path.isfile(SSN2SPLITER_DLL_PATH):
    raise FileNotFoundError(
        f"Couldn't find SSN2 Splitter DLL at {SSN2SPLITER_DLL_PATH}, please check that "
        f"Atlas 10 is installed."
    )

clr.AddReference(SSN2SPLITER_DLL_PATH)  # pylint: disable=no-member

from System.Collections.Generic import (  # .NET imports, so pylint: disable=wrong-import-position,wrong-import-order,import-error,wildcard-import
    List as NETList,
)
from System.Collections.ObjectModel import *  # .NET imports, so pylint: disable=wrong-import-position,wrong-import-order,import-error,wildcard-import
from System import *  # .NET imports, so pylint: disable=wrong-import-position,wrong-import-order,import-error,wildcard-import

from MAT.OCS.Core import *  # .NET imports, so pylint: disable=wrong-import-position,wrong-import-order,import-error,wildcard-import
from MAT.SqlRace.Ssn2Splitter import (  # .NET imports, so pylint: disable=wrong-import-position,wrong-import-order,import-error,wildcard-import,unused-import
    Ssn2SessionExporter,
)
from MESL.SqlRace.Domain import *  # .NET imports, so pylint: disable=wrong-import-position,wrong-import-order,import-error,wildcard-import
from MESL.SqlRace.Enumerators import *  # .NET imports, so pylint: disable=wrong-import-position,wrong-import-order,import-error,wildcard-import
from MESL.SqlRace.Domain.Infrastructure.DataPipeline import *  # .NET imports, so pylint: disable=wrong-import-position,wrong-import-order,import-error,wildcard-import


@pd.api.extensions.register_dataframe_accessor("atlas")
class SessionFrame:
    """Extension to interface with ATLAS

    Attributes:
        ApplicationGroupName: Application Group Name
        ParameterGroupIdentifier: Parameter Group Identifier
    """

    def __init__(self, pandas_obj):
        self._obj = pandas_obj
        self.ParameterGroupIdentifier = (  # .NET objects, so pylint: disable=invalid-name
            "SessionFrame"
        )
        self.ApplicationGroupName = (  # .NET objects, so pylint: disable=invalid-name
            "MyApp"
        )
        self.paramchannelID = {}  # .NET objects, so pylint: disable=invalid-name
        self.units = {}  # TODO: move to setter method and check for datatype
        self.descriptions = {}  # TODO: move to setter method and check for datatype
        self.display_format = (
            {}
        )  # TODO: move to setter method and check for datatype and format

    def to_atlas_session(self, session: Session, show_progress_bar: bool = True):
        """Add the contents of the DataFrame to the ATLAS session.

        The index of the DataFrame must be a DatetimeIndex, or else a AttributeError
        will be raised.
        All the data should be as float or can be converted to float.
        A row channel will be created for each column and the parameter will be named
        using the column name.
        If there is a parameter with the same name and app group present in the session,
        it will just add to that existing channel.

        When creating new parameters in the config, Pandlas will utilise df.atlas.unit
        attribute to set the parameter unit. This should be provided in the form of a
        dictionary, where the keys are the parameter identifiers and the values are the
        units, both in string.
        If none have been provided, a default value of no unit "" will be set.

        When creating new parameters in the config, Pandlas will utilise
        df.atlas.description attribute to set the parameter description. This should be
        provided in the form of a dictionary, where the keys are the parameter
        identifiers and the values are the units, both in string.
        If none have been provided, a default value of f"{parameter_name} description"
        will be set.

        When creating new parameters in the config, Pandlas will utilise
        df.atlas.display_format attribute to set the parameter format override. This
        should be provided in the form of a dictionary, where the keys are the parameter
         identifiers and the values are the units, both in string. The format override
         must be in a valid form.
        If none have been provided, a default value of "%5.2f" will be set.

        Args:
            session: MESL.SqlRace.Domain.Session to the data to.
            show_progress_bar: Show progress bar when creating config and adding data.
        Raises:
             AttributeError: The index is not a pd.DatetimeIndex.
        """

        if not isinstance(self._obj.index, pd.DatetimeIndex):
            warnings.warn(
                "DataFrame index is not pd.DatetimeIndex, attempting to parse index to "
                "DatetimeIndex."
            )
            try:
                self._obj.index = pd.to_datetime(self._obj.index)
                warnings.warn("parse success.")
            except pd.errors.ParserError:
                warnings.warn("parse failed.")

        if not isinstance(self._obj.index, pd.DatetimeIndex):
            raise AttributeError(
                "DataFrame index is not pd.DatetimeIndex, unable to export to ssn2"
            )

        # remove rows that contain no data at all and sort by time.
        self._obj = self._obj.dropna(axis=1, how="all").sort_index()

        # add a lap at the start of the session
        # TODO: add the rest of the laps
        timestamp = self._obj.index[0]
        timestamp64 = timestamp2long(timestamp)
        try:
            lap = self._obj.loc[timestamp].Lap
        except AttributeError:
            lap = 1
        newlap = Lap(int(timestamp64), int(lap), Byte(0), f"Lap {lap}", True)
        # TODO: what to do when you add to an existing session.
        if session.LapCollection.Count == 0:
            logger.debug("No lap present, automatically adding lap to the start.")
            session.LapCollection.Add(newlap)

        # check if there is config for it already
        need_new_config = False
        for param_name in self._obj.columns:
            param_identifier = f"{param_name}:{self.ApplicationGroupName}"
            if not session.ContainsParameter(param_identifier):
                need_new_config = True

        if need_new_config:
            logger.debug("Creating new config.")
            config_identifier = f"{random.randint(0, 999999):05x}"  # .NET objects, so pylint: disable=invalid-name
            config_decription = "SessionFrame generated config"
            configSetManager = (  # .NET objects, so pylint: disable=invalid-name
                ConfigurationSetManager.CreateConfigurationSetManager()
            )
            config = configSetManager.Create(
                session.ConnectionString, config_identifier, config_decription
            )

            # Add param group
            parameterGroupIdentifier = (  # .NET objects, so pylint: disable=invalid-name
                self.ParameterGroupIdentifier
            )
            group1 = ParameterGroup(parameterGroupIdentifier, parameterGroupIdentifier)
            config.AddParameterGroup(group1)

            # Add app group
            applicationGroupName = (  # .NET objects, so pylint: disable=invalid-name
                self.ApplicationGroupName
            )
            parameterGroupIds = NETList[  # .NET objects, so pylint: disable=invalid-name
                String
            ]()
            parameterGroupIds.Add(  # .NET objects, so pylint: disable=invalid-name
                group1.Identifier
            )
            applicationGroup = (  # .NET objects, so pylint: disable=invalid-name
                ApplicationGroup(
                    applicationGroupName, applicationGroupName, None, parameterGroupIds
                )
            )
            applicationGroup.SupportsRda = False
            config.AddGroup(applicationGroup)

            parameterGroupIdentifier = (  # .NET objects, so pylint: disable=invalid-name
                self.ParameterGroupIdentifier
            )
            applicationGroupName = (  # .NET objects, so pylint: disable=invalid-name
                self.ApplicationGroupName
            )

            # Create channel conversion function
            conversion_function_name = "Simple1To1"
            config.AddConversion(
                RationalConversion.CreateSimple1To1Conversion(
                    conversion_function_name, "", "%5.2f"
                )
            )

            # obtain the data
            for param_name in tqdm(
                self._obj.columns,
                desc="Creating channels",
                disable=not show_progress_bar,
            ):
                param_identifier = f"{param_name}:{self.ApplicationGroupName}"
                # if parameter exists already, then do not create a new parameter
                if session.ContainsParameter(param_identifier):
                    logger.debug(
                        "Parameter identifier already exists: %s.", {param_identifier}
                    )
                    continue

                data = self._obj.loc[:, param_name].dropna().to_numpy()
                dispmax = data.max()
                dispmin = data.min()
                warnmax = dispmax
                warnmin = dispmin

                # Add param channel
                myParamChannelId = ( # .NET objects, so pylint: disable=invalid-name
                    session.ReserveNextAvailableRowChannelId() % 2147483647
                )
                # TODO: this is a stupid workaround because it takes UInt32 but it cast
                #  it to Int32 internally...
                self._add_channel(config, myParamChannelId, param_name)

                #  Add param
                self._add_param(
                    config,
                    applicationGroupName,
                    conversion_function_name,
                    parameterGroupIdentifier,
                    dispmax,
                    dispmin,
                    param_name,
                    warnmax,
                    warnmin,
                )

            try:
                config.Commit()
            except ConfigurationSetAlreadyExistsException:
                logging.warning(
                    "Cannot commit config %s, config already exist.", config.Identifier
                )
            session.UseLoggingConfigurationSet(config.Identifier)

        # Obtain the channel Id for the existing parameters
        for param_name in self._obj.columns:
            param_identifier = f"{param_name}:{self.ApplicationGroupName}"
            if not session.ContainsParameter(param_identifier):
                continue
            param_identifier = f"{param_name}:{self.ApplicationGroupName}"
            parameter = session.GetParameter(param_identifier)
            if parameter.Channels.Count != 1:
                logger.warning(
                    "Parameter %s contains more than 1 channel.", param_identifier
                )
            self.paramchannelID[param_name] = parameter.Channels[0].Id

        # write it to the session
        # TODO: make a separate version of to_atlas to specify workers
        # with tqdm(total=self._obj.shape[1], desc="Adding data", disable=not show_progress_bar) as pbar:
        #     for i in self._obj.columns:
        #         self.__add_data_for_param(i,session,pbar)

        with tqdm(total=self._obj.shape[1], desc="Adding data", disable=not show_progress_bar) as pbar:
            with ThreadPoolExecutor(max_workers=20) as executor:
                input_args = [[i, session, pbar] for i in self._obj.columns]
                future_tasks = [executor.submit(self.__add_data_for_param, *args) for args in input_args]
                wait(future_tasks)
                print("taskDone")

        logging.debug(
            "Data for %s:%s added.",
            self.ParameterGroupIdentifier,
            self.ApplicationGroupName,
        )

    def __add_data_for_param(self, param_name, session, progress_bar=None):
        series = self._obj.loc[:, param_name].dropna()
        timestamps = series.index
        data = series.to_numpy()
        myParamChannelId = self.paramchannelID[  # .NET objects, so pylint: disable=invalid-name
            param_name
        ]
        self.add_row_data(session, myParamChannelId, data, timestamps)
        if progress_bar is not None:
            progress_bar.update(1)

    def _add_param(
        self,
        config: ConfigurationSet,
        ApplicationGroupName: str,  # .NET objects, so pylint: disable=invalid-name
        ConversionFunctionName: str,  # .NET objects, so pylint: disable=invalid-name
        ParameterGroupIdentifier: str,  # .NET objects, so pylint: disable=invalid-name
        display_max: float,
        display_min: float,
        parameter_name: str,
        warning_max: float,
        warning_min: float,
    ):
        """Adds a parameter to the ConfigurationSet.

        Args:
            config: ConfigurationSet to add to.
            ApplicationGroupName: Name of the ApplicationGroup to be under
            ConversionFunctionName: Name of the conversion factor to apply.
            ParameterGroupIdentifier: ID of the ParameterGroup.
            display_max: Display maximum.
            display_min: Display minimum.
            parameter_name: Parameter name.
            warning_max: Warning maximum.
            warning_min: Warning minimum.

        """
        # TODO: guard again NaNs
        myParamChannelId = NETList[  # .NET objects, so pylint: disable=invalid-name
            UInt32
        ]()
        myParamChannelId.Add(self.paramchannelID[parameter_name])
        parameterIdentifier = f"{parameter_name}:{ApplicationGroupName}"  # .NET objects, so pylint: disable=invalid-name
        parameterGroupIdentifiers = NETList[  # .NET objects, so pylint: disable=invalid-name
            String
        ]()
        parameterGroupIdentifiers.Add(ParameterGroupIdentifier)

        param_description = self.descriptions.get(
            parameterIdentifier, f"{parameter_name} description"
        )
        param_format = self.display_format.get(parameterIdentifier, "%5.2f")
        param_unit = self.units.get(parameterIdentifier, "")

        myParameter = Parameter(  # .NET objects, so pylint: disable=invalid-name
            parameterIdentifier,
            parameter_name,
            param_description,
            float(display_max),
            float(display_min),
            float(warning_max),
            float(warning_min),
            0.0,
            0xFFFF,
            0,
            ConversionFunctionName,
            parameterGroupIdentifiers,
            myParamChannelId,
            ApplicationGroupName,
            param_format,
            param_unit,
        )
        config.AddParameter(myParameter)

    def _add_channel(
            self, config: ConfigurationSet, channel_id: int, parameter_name: str
    ):
        """Adds a row channel to the config.

        Args:
            config: ConfigurationSet to add to.
            channel_id: ID of the channel.
            parameter_name: Name of the parameter.
        """
        self.paramchannelID[parameter_name] = channel_id
        myParameterChannel = Channel(  # .NET objects, so pylint: disable=invalid-name
            channel_id,
            "MyParamChannel",
            0,
            DataType.FloatingPoint32Bit,
            ChannelDataSourceType.RowData,
        )
        config.AddChannel(myParameterChannel)

    def add_row_data(
            self,
            session: Session,
            channel_id: float,
            data: np.ndarray,
            timestamps: Union[pd.DatetimeIndex, npt.NDArray[np.datetime64]],
    ):
        """Adds data to a channel.

        Args:
            session: Session to add data to.
            channel_id: ID of the channel.
            data: numpy array of float or float equivalents
            timestamps: timestamps for the datapoints
        """
        # TODO: add in guard against invalid datatypes
        if not isinstance(timestamps, (pd.DatetimeIndex, npt.NDArray[np.datetime64])):
            raise TypeError(
                "timestamps should be pd.DateTimeIndex, "
                "or numpy array of np.datetime64."
            )
        timestamps = timestamp2long(timestamps)

        channelIds = NETList[UInt32]()  # .NET objects, so pylint: disable=invalid-name
        channelIds.Add(channel_id)

        # databytes = data.astype(np.float32).tobytes()
        databytes = bytearray(len(data) * 4)
        for i, value in enumerate(data):
            new_bytes = struct.pack("f", value)
            databytes[i * 4: i * 4 + len(new_bytes)] = new_bytes

        timestamps_array = Array[Int64](len(timestamps))
        for i, timestamp in enumerate(timestamps):
            timestamps_array[i] = Int64(int(timestamp))

        session.AddRowData(channel_id, timestamps_array, databytes, 4, False)
