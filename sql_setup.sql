--------initial setup for simulator -------------


-------Go to the market place and get the POLICY_CHANGE_SIMULATOR APP which is privately shared with you.  if you dont have it, ask becky.oconnor@snowflake.com to create it.



CREATE OR REPLACE DATABASE POLICY_CHANGE_SIMULATOR_STREAMLIT;

CREATE OR REPLACE SCHEMA DATA;

create or replace stage streamlit_stage DIRECTORY = (ENABLE = TRUE);

CREATE OR REPLACE WAREHOUSE POLICY_CHANGE_SIMULATOR_WH WITH WAREHOUSE_SIZE='SMALL';

create or replace stage streamlit_stage DIRECTORY = (ENABLE = TRUE);

CREATE or replace STREAMLIT "Policy Change Simulator"
ROOT_LOCATION = '@policy_change_simulator_streamlit.data.streamlit_stage'
MAIN_FILE = '/Home.py'
QUERY_WAREHOUSE = STREAMLIT;


--------then put all the contents inside the streamlit folder (docs provided) in the streamlit_stage.  Its easier if you use VSCode