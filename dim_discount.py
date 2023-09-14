#%%

from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame
from snowflake.snowpark import functions as fc
from snowflake.snowpark import Column as cl
from snowflake.snowpark import Table
from snowflake.snowpark import Row
from snowflake.snowpark import DataFrameWriter
from snowflake.snowpark import types as tp
import pandas as pd
import numpy as np
import os

#%%

def create_session():

    connection_parameters = {
    "account": "RQ70182.eu-central-1",
    "user": "Quintenbohte",
    "password": "6HtsyM@12345",
    "role": "sysadmin",  # optional
    "warehouse": "COMPUTE_WH",  # optional
    "database": "QUINTEN_DATABASE",  # optional
    "schema": "BRONZE",  # optional
    }

    New_Session = Session.builder.configs(connection_parameters).create()

    return New_Session

session = create_session()
#%%

def Transformations():

    konp = session.table("STAGING.KONP")

    konp_df = konp[['INDEX', 'KONWA', 'BEGDA', 'KNUMA', 'KSTBW', 'SKTOF', 'ENDDA', 'KSCHL', 'KSLVL', 'VATO', 'VAFR']]

    konp_df = konp_df.fillna({"INDEX": -99, "BEGDA": '9999-99-99'})

    return konp_df

def Write_df_to_SF(df, Table_Name):
    df.write.mode("overwrite").save_as_table(Table_Name)

#%%


Table_Name = 'Discount'
df = Transformations()


Write_df_to_SF(df, Table_Name)

