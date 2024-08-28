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
from streamlit_extras.app_logo import add_logo

# Get the current credentials
session = get_active_session()

#load logo




st.set_page_config(
    page_title="DWP Cold Weather Payment Simulator",
    page_icon="👋",
)

st.markdown("""
  <style>
    [data-testid=stSidebar] {
      background-color: #f9fffe;
    }
  </style>
""", unsafe_allow_html=True)



col1,col2,col3 = st.columns([0.3,0.5,0.2])
with col1:
    st.image('images/UK-Gov-logo.jpg')
with col3:
    
    st.image('images/Metoffice.png')




with st.sidebar:


    st.success("Choose Reports after viewing analysis")

   
# Write directly to the app
st.title("Cold Weather Payment Simulator")
st.write(
    """This app models how cold weather payments can effect the uk population and what the estimated cost the cold weather payment would be.  The calculations are based on weather data provided by the met office.
    """
)
##### sidebar to simulate policy changes #######

with st.sidebar:
    st.markdown('##### Select Options')
    with st.form('Drivers'):
        S.shareddata = st.text_input('Database Name from datashare: ','COLD_WEATHER_PAYMENTS_DATASET')
        S.startdate = st.date_input('start date', datetime.date(2022,11,1))
        S.enddate = st.date_input('end date', datetime.date(2023,3,31))
        S.weekly_amount = st.number_input('Weekly Amount £:',0.00,100.00, 25.00)
        S.temperature_threshold = st.number_input('Temperature Threshold °C: ',-4.0,5.0,0.0)
        S.average_over_x_days = st.number_input('Average X days:',1,14,7)
        S.temperature_measure = st.selectbox('choose weather measure: ',['Instantaneous Screen Temperature',
                                                 'Instantaneous Feels Like Temperature (Wind Chil/Heat Stress',
                                                ])
        S.description = st.text_input('Scenario Description:')
        try:
            S.scenario = session.table(f'''{st.experimental_user.user_name}_COLD_WEATHER_PAYMENT_SCENARIO''')\
            .agg(F.max('SCENARIO').alias('SCENARIO')).to_pandas().SCENARIO.iloc[0]+1
            
        except:
            S.scenario = 1
        #focus_measure = st.radio('Measure to visualise:', ['Adults','Under 16s','Number of Households','Min Temp','Total Payable'])
        
        submitted = st.form_submit_button("Save Scenario for more details")


    


if submitted:

    ######## weather data
    summary_data = session.table(f'{S.shareddata}.DATA."Hourly Forecast"')

    #st.dataframe(summary_data.columns)


    hourly_with_date = summary_data.with_column('"Date"',
                         F.date_from_parts(F.substr('Valid Hour',1,4),
                                          F.substr('Valid Hour',5,2),
                                          F.substr('Valid Hour',7,2)))


    hourly_with_date = hourly_with_date.filter(F.col('"Date"').between(S.startdate,S.enddate))\

    hourly_with_date = hourly_with_date.groupBy(F.col('"SSPA Identifier"'),
                         F.col('"Date"')).agg(F.avg(S.temperature_measure).alias('AVERAGE_TEMP'))

    weather_station = session.table(f'{S.shareddata}.DATA.PCSECTORMAPPING')\
    .select('"SiteID"','PC_SECT','LONG','LAT')\
    .with_column('Postcode_Area',F.call_function('SPLIT_PART',F.col('PC_SECT'),'_',1)).distinct()



    S.hourly_with_date_ws = hourly_with_date.join(weather_station,on=weather_station['"SiteID"']==hourly_with_date['"SSPA Identifier"'])\
    .group_by('"Date"',
          'POSTCODE_AREA').agg(F.avg(F.cast('LAT',T.FloatType())).alias('LAT'),
                               F.avg(F.cast('LONG',T.FloatType())).alias('LON'),
                               F.avg('AVERAGE_TEMP').alias('AVERAGE_TEMP'))





def movaverage(days,df,startdate):
    window = Window.partition_by(F.col('"POSTCODE_AREA"')).orderBy(F.col('"Date"').desc()).rows_between(Window.currentRow,days)

    # Add moving averages columns for Cloud Cover and Solar Energy based on the previously defined window
    df = df.with_column(f'"Temp - Max Temp over {days} Days"',F.cast(F.max("AVERAGE_TEMP").over(window),T.FloatType())).sort('"Date"')
    
    
    # Change the data type to a float
    df = df.with_column('"AVERAGE_TEMP"',F.cast('"AVERAGE_TEMP"',T.FloatType()))
    df = df.filter(F.col(f'"Temp - Max Temp over {days} Days"')<=S.temperature_threshold)
    df = df.with_column('datediff',F.datediff('day',F.lit(S.startdate),F.col('"Date"')))
    
    return df



if submitted:
    movavg = movaverage(S.average_over_x_days,S.hourly_with_date_ws,S.startdate)
    movavg = movavg.with_column('ROW_NUM',(F.row_number().over(Window.partition_by(F.col("POSTCODE_AREA")).order_by(F.col('"Date"')))))
    
    #check to see if the number is divisible by 7 days as 3x days in a row would still be 1 payment of 25
    movavg = movavg.with_column('div_multiple',(F.to_variant(F.call_function('MOD',(F.col('ROW_NUM'),F.lit(7))))))
    movavg = movavg.filter((F.col('ROW_NUM')==1)|(F.col('DIV_MULTIPLE')==0))
    movavg = movavg.with_column('Payable Amount',F.lit(S.weekly_amount))

    payable_postcode = movavg.group_by('POSTCODE_AREA')\
    .agg(F.any_value('LAT').alias('LAT'),
         F.any_value('LON').alias('LON'),
        F.min('AVERAGE_TEMP').alias('Min Temp'),
         F.max(f'"Temp - Max Temp over {S.average_over_x_days} Days"').alias(f'"Temp - Max Temp over {S.average_over_x_days} Days"'),
         F.sum('"Payable Amount"').alias('Payable Amount'))


    population = session.table(f'{S.shareddata}.DATA."Synthetic Population"')
    #only_not_working
    population_not_working = population.filter(F.col('OCCUPATION_CODE')==2)
    #exclude children and not working 
    population_working = population.filter((F.col('OCCUPATION_CODE')!=2)&(F.col('OCCUPATION_CODE')!=1))
    S.children = population.filter((F.col('OCCUPATION_CODE')==1))

    household_children = S.children.group_by(F.col('HOUSEHOLD'),F.col('POSTCODE')).agg(F.count('*').alias('Children'))

    #working household
    working_household = population_working\
                 .select('HOUSEHOLD','NI NUMBER')\
                 .group_by(F.col('HOUSEHOLD'))\
                 .agg(F.count('*').alias('WORKING_PEOPLE'))

    #those entitled to coldweather_payments
    S.population_entitled_cold_weather = population_not_working.join(working_household, on=(population_not_working['HOUSEHOLD']==working_household['HOUSEHOLD']), how='outer',rsuffix='_L').drop('HOUSEHOLD_L')\
    .filter(F.col('WORKING_PEOPLE').isNull()).drop('WORKING_PEOPLE')



    #households entitled to coldweather payments
    S.households_cold_weather = S.population_entitled_cold_weather\
                       .with_column('ELECTRICITY_BILL_PAYER'
                       ,F.concat('FIRST_NAME',F.lit(' ')
                       ,'LAST_NAME')).group_by('HOUSEHOLD'
                       ,'POSTCODE')\
    .agg(F.any_value('ELECTRICITY_BILL_PAYER').alias('HOUSEHOLD_BILL_PAYER')
     ,F.count('*').alias('Adults'))\
    .with_column('POSTCODE_AREA',F.to_char(F.split(F.col('POSTCODE'),F.lit(' '))[0]))


    S.households_cold_weather = S.households_cold_weather.join(household_children,on='HOUSEHOLD',how='outer',rsuffix='C')

    S.households_cold_weather=S.households_cold_weather.group_by('POSTCODE_AREA').agg(F.sum('Adults').alias('"Adults"'), 
                                                                              F.count('*').alias('Number of Households'),F.count('Children').alias('Under 16s'))






    S.entitled_payments = S.households_cold_weather.join(payable_postcode,on='POSTCODE_AREA')
    S.entitled_payments = S.entitled_payments.with_column_renamed('POSTCODE_AREA','Postcode Area')

    S.entitled_payments = S.entitled_payments.with_column('Total Payable',F.col('Payable Amount')*F.col('Number of Households'))



    ##### save payments
    entitled_payments_sav = S.entitled_payments.with_column('Timestamp',
                                                    F.current_timestamp())\
                                .with_column('Current User',
                                        F.lit(st.experimental_user.user_name)).with_column('Scenario',
                                                            F.lit(str(S.scenario))).with_column('Scenario Description',
                                                                                                        F.lit(S.description))

    entitled_payments_sav = entitled_payments_sav.with_column('Options: Start Date',F.lit(S.startdate))
    entitled_payments_sav = entitled_payments_sav.with_column('Options: End Date',F.lit(S.enddate))
    entitled_payments_sav = entitled_payments_sav.with_column('Options: Weekly Amount £',F.lit(S.weekly_amount))
    entitled_payments_sav = entitled_payments_sav.with_column('Options: Temperature Threshold',F.lit(S.temperature_threshold))
    entitled_payments_sav = entitled_payments_sav.with_column('Options: Average X Days',F.lit(S.average_over_x_days))
    entitled_payments_sav = entitled_payments_sav.with_column('Options: Weather Measure',F.lit(S.temperature_measure))


    session.sql(f'''CREATE TABLE IF NOT EXISTS "POLICY_CHANGE_SIMULATOR_STREAMLIT.DATA.{st.experimental_user.user_name}_COLD_WEATHER_PAYMENT_SCENARIO" (
	"Postcode Area" VARCHAR(16777216),
	"Adults" NUMBER(38,0),
	"Number of Households" NUMBER(38,0) NOT NULL,
	"Under 16s" NUMBER(38,0) NOT NULL,
	LAT FLOAT,
	LON FLOAT,
	"Min Temp" FLOAT,
	"Temp - Max Temp over 7 Days" FLOAT,
	"Payable Amount" FLOAT,
	"Total Payable" FLOAT,
	TIMESTAMP TIMESTAMP_LTZ(9) NOT NULL,
	"Current User" VARCHAR(500) NOT NULL,
	SCENARIO NUMBER(30,0) NOT NULL,
    "Scenario Description" VARCHAR(150),
	"Options: Start Date" DATE NOT NULL,
	"Options: End Date" DATE NOT NULL,
	"Options: Weekly Amount £" FLOAT NOT NULL,
	"Options: Temperature Threshold" NUMBER(38,1) NOT NULL,
	"Options: Average X Days" NUMBER(38,0) NOT NULL,
	"Options: Weather Measure" VARCHAR(500) NOT NULL
)''').collect()
    entitled_payments_sav.write.save_as_table(f'POLICY_CHANGE_SIMULATOR_STREAMLIT.DATA.{st.experimental_user.user_name}_COLD_WEATHER_PAYMENT_SCENARIO',mode="append")


    #st.dataframe(entitled_payments)
    

    S.scenario = session.table(f'''POLICY_CHANGE_SIMULATOR_STREAMLIT.DATA.{st.experimental_user.user_name}_COLD_WEATHER_PAYMENT_SCENARIO''')\
            .agg(F.max('SCENARIO').alias('SCENARIO')).to_pandas().SCENARIO.iloc[0]

    st.markdown(f'#### Your Scenario has saved as version number {S.scenario}')

    col1,col2,col3,col4 = st.columns(4)
    with col1:
        st.metric('Payments - £Mil: ', 
            round(S.entitled_payments.agg(F.sum('Total Payable').alias('TOTAL')).to_pandas()\
                    .TOTAL.iloc[0]/1000000,2))

    with col2:
        st.metric('Households - £Thous: ', 
            round(S.entitled_payments.agg(F.sum('"Number of Households"').alias('TOTAL')).to_pandas()\
                    .TOTAL.iloc[0]/1000,2))

    with col3:
        st.metric('Adults - £Thous: ', 
            round(S.entitled_payments.agg(F.sum('"Adults"').alias('TOTAL')).to_pandas()\
                    .TOTAL.iloc[0]/1000,2))

    with col4:
        st.metric('Under 16s - £Thous: ', 
            round(S.entitled_payments.agg(F.sum('"Under 16s"').alias('TOTAL')).to_pandas()\
                    .TOTAL.iloc[0]/1000,2))



    st.divider()

    S.geos = session.table(f'{S.shareddata}.DATA."Postal Out Code Geometries"')

    st.markdown('#### Postcode areas effected by the payment policy')

    joined = S.entitled_payments.join(S.geos,on=S.entitled_payments['"Postcode Area"']==S.geos['"Name"'],lsuffix='_e')

    joined = joined.drop('LAT_E','LON_E')
    #st.dataframe(joined.columns)

    bet1, bet2,bet3 = st.columns([0.15,0.4,0.15])

    with bet2:
        geom = gpd.GeoDataFrame(S.geos.to_pandas())
        geodframe = geom.set_geometry(gpd.GeoSeries.from_wkt(geom['WKT']))
        geodframe.crs = "EPSG:4326"

        geom = gpd.GeoDataFrame(joined.to_pandas())
        geodframe2 = geom.set_geometry(gpd.GeoSeries.from_wkt(geom['WKT']))
        geodframe2.crs = "EPSG:4326"



        fig, ax = plt.subplots(1, figsize=(10, 5))
        ax.axis('off')
        geodframe.crs = "EPSG:4326"
        geodframe.plot(color='white',alpha=0.8,ax=ax, edgecolors='grey',linewidth=0.2, figsize=(9, 10))
        geodframe2.plot(color='#31b1c9', alpha=1,ax=ax, figsize=(9, 10))
        st.pyplot(fig)