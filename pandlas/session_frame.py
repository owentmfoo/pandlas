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
from typing import Union
import logging
import numpy.typing as npt
import numpy as np
import pandas as pd
from tqdm import tqdm
from pandlas.utils import timestamp2long

A10_INSTALL_PATH = r"C:\Program Files\McLaren Applied Technologies\ATLAS 10"
# configure pythonnet runtime for SQLRace API
os.environ["PYTHONNET_RUNTIME"] = "coreclr"
os.environ["PYTHONNET_CORECLR_RUNTIME_CONFIG"] = (
    rf"{A10_INSTALL_PATH}\MAT.Atlas.Host.runtimeconfig.json"
)
# only import clr after the runtime has been configured, so pylint: disable=wrong-import-position
import clr

logger = logging.getLogger(__name__)

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

from System import (  # .NET imports, so pylint: disable=wrong-import-position,wrong-import-order,import-error,wildcard-import
    Byte,
    String,
    UInt32,
    Array,
    Int64,
)
from MESL.SqlRace.Domain import (  # .NET imports, so pylint: disable=wrong-import-position,wrong-import-order,import-error,wildcard-import
    Session,
    Lap,
    ConfigurationSetManager,
    ParameterGroup,
    ApplicationGroup,
    RationalConversion,
    ConfigurationSetAlreadyExistsException,
    ConfigurationSet,
    Parameter,
    Channel,
)
from MESL.SqlRace.Enumerators import (  # .NET imports, so pylint: disable=wrong-import-position,wrong-import-order,import-error,wildcard-import
    DataType,
    ChannelDataSourceType,
)


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
        self.units = {}
        self.descriptions = {}
        self.display_format = {}
        self.display_limits = {}
        self.warning_limits = {}
        self.parameter_group_separator = None  # e.g. "/" to split "Chassis/DamperFL"

    def _resolve_param_group(self, column_name: str) -> tuple[str, str]:
        """Resolve the parameter group and clean parameter name for a column.

        When ``parameter_group_separator`` is set, the column name is split on
        the separator.  The first part becomes the parameter group identifier
        and the second part becomes the parameter name.

        When the separator is ``None`` (default), the column name is used as-is
        and the default ``ParameterGroupIdentifier`` is returned.

        Returns:
            ``(parameter_group, parameter_name)``
        """
        sep = self.parameter_group_separator
        if sep and sep in column_name:
            parts = column_name.split(sep, 1)
            return parts[0], parts[1]
        return self.ParameterGroupIdentifier, column_name

    def to_atlas_session(self, session: Session, show_progress_bar: bool = True):
        """Add the contents of the DataFrame to the ATLAS session.

        The index of the DataFrame must be a DatetimeIndex, or else a AttributeError
        will be raised.
        All the data should be as float or can be converted to float.
        A row channel will be created for each column and the parameter will be named
        using the column name.
        If there is a parameter with the same name and app group present in the session,
        it will just add to that existing channel.

        Parameter metadata can be customised via the following attributes, all provided
        as dictionaries keyed by parameter identifier
        (``"{column_name}:{ApplicationGroupName}"``):

        - ``df.atlas.units``: Unit string for each parameter (default ``""``).
        - ``df.atlas.descriptions``: Description string (default
          ``"{parameter_name} description"``).
        - ``df.atlas.display_format``: Printf-style format override (default
          ``"%5.2f"``).
        - ``df.atlas.display_limits``: ``(min, max)`` tuple overriding the display
          range. When not set, the actual data min/max is used.
        - ``df.atlas.warning_limits``: ``(min, max)`` tuple overriding the warning
          range. When not set, the display limits are used.

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
        # Build resolved mapping: column_name -> (group, clean_param_name)
        col_group_map = {}
        for col in self._obj.columns:
            grp, clean = self._resolve_param_group(col)
            col_group_map[col] = (grp, clean)

        for col in self._obj.columns:
            _, clean = col_group_map[col]
            param_identifier = f"{clean}:{self.ApplicationGroupName}"
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

            # Discover unique parameter groups from columns
            unique_groups = list(dict.fromkeys(
                grp for grp, _ in col_group_map.values()
            ))

            # Add all parameter groups
            parameterGroupIds = NETList[String]()
            for grp_name in unique_groups:
                pg = ParameterGroup(grp_name, grp_name)
                config.AddParameterGroup(pg)
                parameterGroupIds.Add(pg.Identifier)

            # Add app group with all parameter groups
            applicationGroupName = self.ApplicationGroupName
            applicationGroup = ApplicationGroup(
                applicationGroupName, applicationGroupName, None, parameterGroupIds
            )
            applicationGroup.SupportsRda = False
            config.AddGroup(applicationGroup)

            # Create channel conversion function
            conversion_function_name = "Simple1To1"
            config.AddConversion(
                RationalConversion.CreateSimple1To1Conversion(
                    conversion_function_name, "", "%5.2f"
                )
            )

            # obtain the data
            for col in tqdm(
                self._obj.columns,
                desc="Creating channels",
                disable=not show_progress_bar,
            ):
                grp, clean = col_group_map[col]
                param_identifier = f"{clean}:{self.ApplicationGroupName}"
                # if parameter exists already, then do not create a new parameter
                if session.ContainsParameter(param_identifier):
                    logger.debug(
                        "Parameter identifier already exists: %s.", {param_identifier}
                    )
                    continue

                data = self._obj.loc[:, col].dropna().to_numpy()
                dispmax = data.max()
                dispmin = data.min()
                warnmax = dispmax
                warnmin = dispmin

                # Add param channel
                myParamChannelId = (  # .NET objects, so pylint: disable=invalid-name
                    session.ReserveNextAvailableRowChannelId() % 2147483647
                )
                # TODO: this is a stupid workaround because it takes UInt32 but it cast
                #  it to Int32 internally...
                self._add_channel(config, myParamChannelId, col)

                #  Add param
                self._add_param(
                    config,
                    applicationGroupName,
                    conversion_function_name,
                    grp,
                    dispmax,
                    dispmin,
                    col,
                    warnmax,
                    warnmin,
                )

            try:
                config.Commit()
            except ConfigurationSetAlreadyExistsException:
                logger.warning(
                    "Cannot commit config %s, config already exist.", config.Identifier
                )
            session.UseLoggingConfigurationSet(config.Identifier)

        # Obtain the channel Id for the existing parameters
        for col in self._obj.columns:
            _, clean = col_group_map[col]
            param_identifier = f"{clean}:{self.ApplicationGroupName}"
            if not session.ContainsParameter(param_identifier):
                continue
            parameter = session.GetParameter(param_identifier)
            if parameter.Channels.Count != 1:
                logger.warning(
                    "Parameter %s contains more than 1 channel.", param_identifier
                )
            self.paramchannelID[col] = parameter.Channels[0].Id

        # write it to the session
        for col in tqdm(
            self._obj.columns, desc="Adding data", disable=not show_progress_bar
        ):
            series = self._obj.loc[:, col].dropna()
            timestamps = series.index
            data = series.to_numpy()
            myParamChannelId = (    # .NET objects, so pylint: disable=invalid-name
                self.paramchannelID[col]
            )
            self.add_data(session, myParamChannelId, data, timestamps)

        logger.debug(
            "Data for %s:%s added.",
            self.ParameterGroupIdentifier,
            self.ApplicationGroupName,
        )

    def _add_param(
        self,
        config: ConfigurationSet,
        ApplicationGroupName: str,  # .NET objects, so pylint: disable=invalid-name
        ConversionFunctionName: str,  # .NET objects, so pylint: disable=invalid-name
        ParameterGroupIdentifier: str,  # .NET objects, so pylint: disable=invalid-name
        display_max: float,
        display_min: float,
        column_name: str,
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
            column_name: Original DataFrame column name (used to look up channel ID).
            warning_max: Warning maximum.
            warning_min: Warning minimum.

        """
        _, clean_name = self._resolve_param_group(column_name)
        # TODO: guard again NaNs
        myParamChannelId = NETList[  # .NET objects, so pylint: disable=invalid-name
            UInt32
        ]()
        myParamChannelId.Add(self.paramchannelID[column_name])
        parameterIdentifier = f"{clean_name}:{ApplicationGroupName}"  # .NET objects, so pylint: disable=invalid-name
        parameterGroupIdentifiers = NETList[  # .NET objects, so pylint: disable=invalid-name
            String
        ]()
        parameterGroupIdentifiers.Add(ParameterGroupIdentifier)

        # Metadata lookup: try both clean identifier and full column-based identifier
        full_identifier = f"{column_name}:{ApplicationGroupName}"
        param_description = self.descriptions.get(
            parameterIdentifier,
            self.descriptions.get(
                full_identifier, f"{clean_name} description"
            ),
        )
        param_format = self.display_format.get(
            parameterIdentifier,
            self.display_format.get(full_identifier, "%5.2f"),
        )
        param_unit = self.units.get(
            parameterIdentifier,
            self.units.get(full_identifier, ""),
        )

        param_display_limits = self.display_limits.get(
            parameterIdentifier,
            self.display_limits.get(full_identifier, None),
        )
        if param_display_limits is not None:
            display_min, display_max = param_display_limits

        param_warning_limits = self.warning_limits.get(
            parameterIdentifier,
            self.warning_limits.get(full_identifier, None),
        )
        if param_warning_limits is not None:
            warning_min, warning_max = param_warning_limits

        myParameter = Parameter(  # .NET objects, so pylint: disable=invalid-name
            parameterIdentifier,
            clean_name,
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

    def add_data(
        self,
        session: Session,
        channel_id: int,
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

        databytes = bytes(data.astype(np.float32).tobytes())

        timestamps_array = Array[Int64](timestamps.astype(np.int64).tolist())

        session.AddRowData(channel_id, timestamps_array, databytes, 4, False)
