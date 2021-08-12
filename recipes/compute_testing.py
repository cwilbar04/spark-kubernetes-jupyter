# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu



# Compute recipe outputs
# TODO: Write here your actual code that computes the outputs
# NB: DSS supports several kinds of APIs for reading and writing data. Please see doc.
d = {'col1': [1, 2], 'col2': [3, 4]}
testing_df = pd.DataFrame(data=d)


# Write recipe outputs
testing = dataiku.Dataset("testing")
testing.write_with_schema(testing_df)
