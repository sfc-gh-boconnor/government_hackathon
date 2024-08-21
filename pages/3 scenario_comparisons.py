# Import python packages
import streamlit as st
from streamlit import session_state as S
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import functions as F
import datetime
from snowflake.snowpark import types as T
from snowflake.snowpark.window import Window
import altair as alt
import matplotlib.pyplot as plt
import geopandas as gpd
from matplotlib.colors import LinearSegmentedColormap, ListedColormap


# Get the current credentials
session = get_active_session()

#load logo

col1,col2,col3 = st.columns(3)
with col1:
    st.image('images/UK-Gov-logo.jpg')

st.title("Scenario Comparison")

try:

    #functions
    @st.cache_data
    def geos1():
        
        return session.table(f'{S.shareddata}.DATA."Postal Out Code Geometries"').to_pandas()

    @st.cache_data
    def geos2(SELECT_SCENARIO):
        data = session.table(f'{st.experimental_user.user_name}_COLD_WEATHER_PAYMENT_SCENARIO').filter(F.col('SCENARIO')==SELECT_SCENARIO)
        geos = session.table(f'{S.shareddata}.DATA."Postal Out Code Geometries"')
        return geos.join(data,on=geos['"Name"']==data['"Postcode Area"'],lsuffix='L').to_pandas()

    @st.cache_data
    def datapd(SELECT_SCENARIO):
        return session.table(f'{st.experimental_user.user_name}_COLD_WEATHER_PAYMENT_SCENARIO').filter(F.col('SCENARIO')==SELECT_SCENARIO).to_pandas()

    @st.cache_data
    def data_for_charts(SELECT_SCENARIO):
        data = session.table(f'{st.experimental_user.user_name}_COLD_WEATHER_PAYMENT_SCENARIO').filter(F.col('SCENARIO')==SELECT_SCENARIO)
        return data.to_pandas()

    @st.cache_data
    def data_for_top_5(SELECT_SCENARIO,measure):
        data = session.table(f'{st.experimental_user.user_name}_COLD_WEATHER_PAYMENT_SCENARIO').filter(F.col('SCENARIO')==SELECT_SCENARIO)\
        .sort(F.col(f'"{measure}"').desc()).limit(5)
        return data.to_pandas()

    def datasp(SELECT_SCENARIO):
        return session.table(f'{st.experimental_user.user_name}_COLD_WEATHER_PAYMENT_SCENARIO').filter(F.col('SCENARIO')==SELECT_SCENARIO)

    def viewmap(SELECT_SCENARIO):
        geos = session.table(f'{S.shareddata}.DATA."Postal Out Code Geometries"')
        geo_with_data = geos
        geom = geos1()
        geodframe = geom.set_geometry(gpd.GeoSeries.from_wkt(geom['WKT']))
        geodframe.crs = "EPSG:4326"
        geom = geos2(SELECT_SCENARIO)

        geodframe2 = geom.set_geometry(gpd.GeoSeries.from_wkt(geom['WKT']))
        geodframe2.crs = "EPSG:4326"
        fig, ax = plt.subplots(1, figsize=(10, 5))
        ax.axis('off')
        geodframe.plot(color='white',alpha=0.8,ax=ax, edgecolors='grey',linewidth=0.2, figsize=(9, 10))
        geodframe2.plot(color='#00c5b6', alpha=1,ax=ax, figsize=(9, 10))
        return st.pyplot(fig)

    def barchart(data):
        d = alt.Chart(data).mark_bar(color='#00c5b6').encode(
        alt.X('Postcode Area:N', sort=alt.EncodingSortField(field=measure,order='descending')),
        alt.Y(f'{measure}:Q'))
        return st.altair_chart(d, use_container_width=True)

    def scatterchart(SELECT_SCENARIO):
        c = (alt.Chart(data_for_charts(SELECT_SCENARIO)).mark_circle(color='#00c5b6').encode(
                    x='Min Temp',
                    y= measure,
                    #color= alt.color('#31b1c9'),
                    tooltip=['Postcode Area', 'Min Temp'])).interactive()
        return st.altair_chart(c, use_container_width=True)
    
    with st.sidebar:

        SCENARIO = session.table(f'{st.experimental_user.user_name}_COLD_WEATHER_PAYMENT_SCENARIO').select('SCENARIO').distinct().to_pandas()
    
        with st.form('Scenarios'):
            
            SELECT_SCENARIO_1 = st.selectbox('Choose Scenario 1: ', SCENARIO)
            SELECT_SCENARIO_2 = st.selectbox('Choose Scenario 2: ', SCENARIO)
            SELECT_SCENARIO_3 = st.selectbox('Choose Scenario 3: ', SCENARIO)

            measure = st.selectbox('Choose Measure:',['Adults','Number of Households','Under 16s','Total Payable'])
            submitted = st.form_submit_button("Run Comparison Analysis")
 

    if submitted:
    


        st.markdown('#### Areas effected by the scenarios')
        col1,col2,col3 = st.columns(3)

        
        with col1:
            st.markdown(f'''#### Scenario {SELECT_SCENARIO_1}''')
            st.subheader(datasp(SELECT_SCENARIO_1).agg(F.sum(f'''"{measure}"''').alias(f'''"{measure}"''')).to_pandas()[measure].iloc[0])
            
            viewmap(SELECT_SCENARIO_1)
            barchart(data_for_charts(SELECT_SCENARIO_1))
            scatterchart(SELECT_SCENARIO_1)
            st.divider()
            st.markdown('###### Top 5 Postcodes')
            barchart(data_for_top_5(SELECT_SCENARIO_1,measure))

        with col2:
            st.markdown(f'''#### Scenario {SELECT_SCENARIO_2}''')
            st.subheader(datasp(SELECT_SCENARIO_2).agg(F.sum(f'''"{measure}"''').alias(f'''"{measure}"''')).to_pandas()[measure].iloc[0])
            
            viewmap(SELECT_SCENARIO_2)
            barchart(data_for_charts(SELECT_SCENARIO_2))
            scatterchart(SELECT_SCENARIO_2)
            st.divider()
            st.markdown('###### Top 5 Postcodes')
            barchart(data_for_top_5(SELECT_SCENARIO_2,measure))
            

        with col3:
            st.markdown(f'''#### Scenario {SELECT_SCENARIO_3}''')
            st.subheader(datasp(SELECT_SCENARIO_3).agg(F.sum(f'''"{measure}"''').alias(f'''"{measure}"''')).to_pandas()[measure].iloc[0])

            viewmap(SELECT_SCENARIO_3)
            barchart(data_for_charts(SELECT_SCENARIO_3))
            scatterchart(SELECT_SCENARIO_3)
            st.divider()
            st.markdown('###### Top 5 Postcodes')
            barchart(data_for_top_5(SELECT_SCENARIO_3,measure))
            
 
except:
    st.write('No Data Found - Please Create a Scenario first')