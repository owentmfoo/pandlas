#  Pandlas

A Pandas extension for ATLAS, intended to demonstrate how to use the SQLRace API. 
**This package is not maintained nor officially supported.**

[![linting: pylint](https://img.shields.io/badge/linting-pylint-yellowgreen)](https://github.com/pylint-dev/pylint)


This package utilises API from ATLAS and as such requires a valid ATLAS licence with the SQLRace option included.

## Installtion
```
pip install "git+https://github.com/owentmfoo/pandlas.git#egg=pandlas&subdirectory=pandlas"
```

## Package dependencies
- Pandas
- pythonnet
- tqdm

# Limitations
- Only take a dataframe of float or can be converted to float
- Units are not set for the parameters
- Must have a DateTime index


Further possibilities with SQLRace API but not implemented in this Python package
- Text Channels
- Units for each parameter
- Set custom limits and warnings in ATLAS
- Grouping parameters