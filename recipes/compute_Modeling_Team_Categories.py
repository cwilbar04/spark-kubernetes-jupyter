# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from datetime import datetime

# Read recipe inputs
category_ds = dataiku.Dataset("Contract_Category_Table_20210923")
category_df = category_ds.get_dataframe()

# Compute recipe outputs from inputs

category_df['load_date'] = datetime.now()

# Write recipe outputs
modeling_Team_Categories = dataiku.Dataset("Modeling_Team_Categories")
modeling_Team_Categories.write_with_schema(category_df)
