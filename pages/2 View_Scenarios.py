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



try:

    with st.sidebar:

        SCENARIO = session.table(f'DATA.{st.experimental_user.user_name}_COLD_WEATHER_PAYMENT_SCENARIO').select('SCENARIO').distinct()
    
    
        SELECT_SCENARIO = st.selectbox('Choose Scenario: ', SCENARIO)
 

    
        clear = st.button('Clear All Scenarios')

        if clear:
            session.sql(f'DROP TABLE DATA.{st.experimental_user.user_name}_COLD_WEATHER_PAYMENT_SCENARIO').collect()



    

        data = session.table(f'DATA.{st.experimental_user.user_name}_COLD_WEATHER_PAYMENT_SCENARIO').filter(F.col('SCENARIO')==SELECT_SCENARIO)
        st.markdown(f'''Start Date: {data.select('Options: Start Date').distinct().to_pandas()['Options: Start Date'].iloc[0]}''')
        st.markdown(f'''Start Date: {data.select('Options: Start Date').distinct().to_pandas()['Options: Start Date'].iloc[0]}''')
        st.markdown(f'''Description: {data.select('Scenario Description').distinct().to_pandas()['Scenario Description'].iloc[0]}''')
        st.markdown(f'''Weekly Amount: {data.select('Options: Weekly Amount £').distinct().to_pandas()['Options: Weekly Amount £'].iloc[0]}''')
        st.markdown(f'''Temperature Threshold: {data.select('Options: Temperature Threshold').distinct().to_pandas()['Options: Temperature Threshold'].iloc[0]}''')
        st.markdown(f'''Average X Days: {data.select('Options: Average X Days').distinct().to_pandas()['Options: Average X Days'].iloc[0]}''')
        st.markdown(f'''Weather Measure: {data.select('Options: Weather Measure').distinct().to_pandas()['Options: Weather Measure'].iloc[0]}''')

    col1,col2,col3 = st.columns(3)
    with col1:
        st.image('images/UK-Gov-logo.jpg')


    st.title("Scenario Analysis")

    

    st.markdown('#### Areas effected by the scenario')
    col1,col2,col3 = st.columns([0.3,0.4,0.3])

    @st.cache_data
    def geos1():
        
        return session.table(f'{S.shareddata}.DATA."Postal Out Code Geometries"').to_pandas()

    @st.cache_data
    def geos2(SELECT_SCENARIO):
        data = session.table(f'DATA.{st.experimental_user.user_name}_COLD_WEATHER_PAYMENT_SCENARIO').filter(F.col('SCENARIO')==SELECT_SCENARIO)
        geos = session.table(f'{S.shareddata}.DATA."Postal Out Code Geometries"')
        return geos.join(data,on=geos['"Name"']==data['"Postcode Area"'],lsuffix='L').to_pandas()
        
    with col2:
        geos = session.table(f'{S.shareddata}.DATA."Postal Out Code Geometries"')
        geo_with_data = geos
        geom = geos1()
        geodframe = geom.set_geometry(gpd.GeoSeries.from_wkt(geom['WKT']))
        geodframe.crs = "EPSG:4326"

        #geo_with_data = geos.join(data,on=geos['"Name"']==data['"Postcode Area"'],lsuffix='L')
        #geom = geo_with_data.to_pandas()
        geom = geos2(SELECT_SCENARIO)

        geodframe2 = geom.set_geometry(gpd.GeoSeries.from_wkt(geom['WKT']))
        geodframe2.crs = "EPSG:4326"



        fig, ax = plt.subplots(1, figsize=(10, 5))
        ax.axis('off')
        geodframe.crs = "EPSG:4326"
        geodframe.plot(color='white',alpha=0.8,ax=ax, edgecolors='grey',linewidth=0.2, figsize=(9, 10))
        geodframe2.plot(color='#00c5b6', alpha=1,ax=ax, figsize=(9, 10))
        st.pyplot(fig)


    measure = st.selectbox('Choose Measure:',['Adults','Number of Households','Under 16s','Min Temp','Total Payable'])
    col1,col2 = st.columns(2)

    @st.cache_data
    def data_for_charts(SELECT_SCENARIO):
        data = session.table(f'DATA.{st.experimental_user.user_name}_COLD_WEATHER_PAYMENT_SCENARIO').filter(F.col('SCENARIO')==SELECT_SCENARIO)
        return data.to_pandas()
    
    with col1:

        
        d = alt.Chart(data_for_charts(SELECT_SCENARIO)).mark_bar(color='#00c5b6').encode(
        alt.X('Postcode Area:N', sort=alt.EncodingSortField(field=measure,order='descending')),
        alt.Y(f'{measure}:Q'))

        st.altair_chart(d, use_container_width=True)
    with col2:
        c = (alt.Chart(data_for_charts(SELECT_SCENARIO)).mark_circle(color='#31b1c9').encode(
                    x='Min Temp',
                    y= measure,
                    #color= alt.color('#31b1c9'),
                    tooltip=['Postcode Area', 'Min Temp'])
    ).interactive()

        st.altair_chart(c, use_container_width=True)

        

        #st.markdown('#### Summary Data')

        #st.dataframe(data)
    
except:
    st.write('No Data Found - Please Create a Scenario first')