# Import python packages
import streamlit as st
from streamlit import session_state as S
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import functions as F
import datetime
from snowflake.snowpark import types as T
from snowflake.snowpark.window import Window
import altair as alt

# Get the current credentials
session = get_active_session()



st.set_page_config(layout="wide")

st.write(session.get_current_role())




def barchart(data,measure):
    d = alt.Chart(data).mark_bar(color='#00c5b6').encode(
    alt.X('Postcode Area:N', sort=alt.EncodingSortField(field=measure,order='descending')),
        alt.Y(f'{measure}:Q'))
    return st.altair_chart(d, use_container_width=True)




col1,col2,col3 = st.columns(3)
with col1:
    st.image('images/UK-Gov-logo.jpg')


st.markdown('# DETAILS OF AFFECTED PEOPLE FOR SCENARIO')


SCENARIOS = session.table(f'DATA.{st.experimental_user.user_name}_COLD_WEATHER_PAYMENT_SCENARIO')
SCENARIO = SCENARIOS.select('SCENARIO').distinct()
Postcode = SCENARIOS.select('"Postcode Area"').distinct()


chosen = st.selectbox('Choose Scenario:',SCENARIO)




SELECTEDsp = SCENARIOS.filter(F.col('SCENARIO')== chosen)
SELECTED = SELECTEDsp.to_pandas()

st.markdown(f'''###### Scenario Comments: {SELECTED['Scenario Description'].iloc[0]}''')
col1,col2,col3,col4,col5 = st.columns(5)


with col1:
    usix = SELECTEDsp.sort(F.col('"Adults"').desc()).limit(5).to_pandas()
    barchart(usix,'Adults')

with col2:
    usix = SELECTEDsp.sort(F.col('"Under 16s"').desc()).limit(5).to_pandas()
    barchart(usix,'Under 16s')

with col3:
    usix = SELECTEDsp.sort(F.col('"Min Temp"').desc()).limit(5).to_pandas()
    barchart(usix,'Min Temp')

with col4:
    usix = SELECTEDsp.sort(F.col('"Total Payable"').desc()).limit(5).to_pandas()
    barchart(usix,'Total Payable')

with col5:
    usix = SELECTEDsp.sort(F.col('"Number of Households"').desc()).limit(5).to_pandas()
    barchart(usix,'Number of Households')


chosen2 = st.selectbox('Choose Postcode:',Postcode)


st.markdown('#### People entitled to cold weather payments in above scenario')

population = session.table(f'{S.shareddata}.DATA."Synthetic Population"')
#only_not_working
population_not_working = population.filter(F.col('OCCUPATION_CODE')==2)
#exclude children and not working 
population_working = population.filter((F.col('OCCUPATION_CODE')!=2)&(F.col('OCCUPATION_CODE')!=1))
children = population.filter((F.col('OCCUPATION_CODE')==1))

household_children = children.group_by(F.col('HOUSEHOLD'),F.col('POSTCODE')).agg(F.count('*').alias('Children'))

#working household
working_household = population_working\
                 .select('HOUSEHOLD','NI NUMBER')\
                 .group_by(F.col('HOUSEHOLD'))\
                 .agg(F.count('*').alias('WORKING_PEOPLE'))

#those entitled to coldweather_payments
population_entitled_cold_weather = population_not_working.join(working_household, on=(population_not_working['HOUSEHOLD']==working_household['HOUSEHOLD']), how='outer',rsuffix='_L').drop('HOUSEHOLD_L')\
    .filter(F.col('WORKING_PEOPLE').isNull()).drop('WORKING_PEOPLE')

filtered_pop = population_entitled_cold_weather.filter(F.split(F.col('POSTCODE'),F.lit(' '))[0]==chosen2)

st.write(filtered_pop.limit(10))