# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu



# Compute recipe outputs
# TODO: Write here your actual code that computes the outputs
# NB: DSS supports several kinds of APIs for reading and writing data. Please see doc.

testing_df = ... # Compute a Pandas dataframe to write into testing


# Write recipe outputs
testing = dataiku.Dataset("testing")
testing.write_with_schema(testing_df)
