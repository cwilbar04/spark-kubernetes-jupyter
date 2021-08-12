# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku.core.sql import SQLExecutor2
import python_recipe_utils as pru

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Recipe inputs - none

# Recipe outputs
CW_SANDBOX_CONTRACT_MONITORING_IL = dataiku.Dataset("CW_SANDBOX_CONTRACT_MONITORING_IL")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Create a single dictionary that contains the recipe variables
client = dataiku.api_client()
proj_key = dataiku.default_project_key()
proj = client.get_project(proj_key)
recipe_vars = proj.get_variables()['standard']

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
recipe_vars

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Write recipe outputs
e = SQLExecutor2(connection=pru.get_connection_name(claim_line_keys))
e.query_to_df(query=insert_output_query,
              pre_queries=volatile_queries+[drop_output_query, create_output_query],
              post_queries=drop_queries+stats_queries)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Write recipe outputs
testing = dataiku.Dataset("testing")
testing.write_with_schema(testing_df)