--------initial setup for simulator -------------


CREATE OR REPLACE DATABASE POLICY_CHANGE_SIMULATOR_STREAMLIT;

CREATE OR REPLACE WAREHOUSE POLICY_CHANGE_SIMULATOR_WH WITH WAREHOUSE_SIZE='SMALL';

CREATE OR REPLACE SCHEMA DATA;

CREATE OR REPLACE SCHEMA NOTEBOOKS;

CREATE OR REPLACE SCHEMA STREAMLITS;

create or replace stage streamlit_stage DIRECTORY = (ENABLE = TRUE);



create or replace stage streamlit_stage DIRECTORY = (ENABLE = TRUE);

CREATE or replace STREAMLIT "Policy Change Simulator"
ROOT_LOCATION = '@policy_change_simulator_streamlit.streamlits.streamlit_stage'
MAIN_FILE = '/Home.py'
QUERY_WAREHOUSE = POLICY_CHANGE_SIMULATOR_WH;