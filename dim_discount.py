#%%

from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame
from snowflake.snowpark import functions as fc
from snowflake.snowpark import Column as cl
from snowflake.snowpark import Table
from snowflake.snowpark import Row
from snowflake.snowpark import DataFrameWriter
from snowflake.snowpark.functions import when_matched, when_not_matched
from snowflake.snowpark import types as tp
import pandas as pd
import numpy as np
import os

#%%

############ SET UP SESSION ##########

def create_session():

    connection_parameters = {
    "account": "RQ70182.eu-central-1",
    "user": "Quintenbohte",
    "password": "6HtsyM@12345",
    "role": "accountadmin",  # optional
    "warehouse": "COMPUTE_WH",  # optional
    "database": "QUINTEN_DATABASE",  # optional
    "schema": "BRONZE",  # optional
    }

    New_Session = Session.builder.configs(connection_parameters).create()

    return New_Session

session = create_session()
session.use_database("QUINTEN_DATABASE")
session.use_schema("BRONZE")
#%%
######## RAW TO BRONZE ########

discount_df = session.table("STAGING.KONP")
discount_df = discount_df[['INDEX', 'KONWA', 'BEGDA', 'KNUMA', 'KSTBW', 'SKTOF', 'ENDDA', 'KSCHL', 'KSLVL', 'VATO', 'VAFR']]
discount_df.show()

## Fill na
values = {
    'INDEX': -99,
    'KONWA': 'NA',
    'BEGDA': '1900-01-01',
    'KNUMA': 'NA',
    'KSTBW': -99,
    'SKTOF': 'NA',
    'ENDDA': '1900-01-01',
    'KSCHL': 'NA',
    'KSLVL': 'NA',
    'VATO': '1900-01-01',
    'VAFR': '1900-01-01',    
}
discount_df = discount_df.fillna(value = values)
discount_df.show()

table_name = 'Discount'
discount_df.write.mode("overwrite").save_as_table(table_name)

#%%
####### BRONZE TO SILVER ###########

discount_bronze = session.table("BRONZE.DISCOUNT")

#Option one. 
#- Create Table and merge new one into the created one

#%%
session.sql('''
        create or replace Table QUINTEN_DATABASE.SILVER.Discount(
                  Index numeric(38,0)
                , DiscountType varchar()
                , BeginDate Timestamp_NTZ
                , DiscountPeriod varchar()
                , Discount numeric(38,0)
                , DiscountID varchar()
                , EndDate Timestamp_NTZ
                , DiscountLevel varchar()
                , DiscountOn varchar()
                , ValidFrom varchar()               
                , ValidTo Timestamp_NTZ
);
''').collect()

#%%
target_silver = session.table("QUINTEN_DATABASE.SILVER.Discount")
discount_silver = discount_bronze
merged_silver = target_silver.merge(discount_silver,
                                (target_silver.index == discount_silver.index), 
                                [when_matched().update({
                                    "DiscountType" :discount_silver["KONWA"], 
                                    "BeginDate" :discount_silver["BEGDA"], 
                                    "DiscountPeriod" :discount_silver["KNUMA"], 
                                    "Discount" :discount_silver["KSTBW"], 
                                    "DiscountID" :discount_silver["SKTOF"], 
                                    "EndDate" :discount_silver["ENDDA"], 
                                    "DiscountLevel" :discount_silver["KSCHL"], 
                                    "DiscountOn" :discount_silver["KSLVL"], 
                                    "ValidFrom" :discount_silver["VATO"], 
                                    "ValidTo" :discount_silver["VAFR"]
                                }),
                                when_not_matched().insert({
                                    "Index" : discount_silver["INDEX"], 
                                    "DiscountType" :discount_silver["KONWA"], 
                                    "BeginDate" :discount_silver["BEGDA"], 
                                    "DiscountPeriod" :discount_silver["KNUMA"], 
                                    "Discount" :discount_silver["KSTBW"], 
                                    "DiscountID" :discount_silver["SKTOF"], 
                                    "EndDate" :discount_silver["ENDDA"], 
                                    "DiscountLevel" :discount_silver["KSCHL"], 
                                    "DiscountOn" :discount_silver["KSLVL"], 
                                    "ValidFrom" :discount_silver["VATO"], 
                                    "ValidTo" :discount_silver["VAFR"]
                                })]
)


# %%
