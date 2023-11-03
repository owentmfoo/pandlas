#  Pandlas

A Pandas extension for ATLAS, intended to demonstrate how to use the SQLRace API. 
**This package is not maintained nor officially supported.** 

This package utilises API from ATLAS and as such requires a valid ATLAS licence with the SQLRace option included.

Package dependencies
- Pandas
- pythonnet
- tqdm

Limitations
- Only take a dataframe of float or can be converted to float
- Units are not set for the parameters
- Must have a DateTime index


Further possibilities with ALTAS API but not implemented in this python package
- Text Channels
- Units for each parameter
- Set custom limits and warnings in ATLAS
- Grouping parameters