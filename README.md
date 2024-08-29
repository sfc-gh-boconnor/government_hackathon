#### GOVERNMENT HACKATHON HANDS ON LAB

##### Set up a free trial account in AWS London

##### Log into Snowflake


Copy your account identifier.  This lab you will be leveraging a private share which I will be supplying

![alt text](image.png)

Open up the following google form and supply your account identifier with your full name and organisation

https://forms.gle/JXF4zSKZP6X26Unr6




I will share all the datasets during your overview session.

> Enjoy your overview session

### HANDS ON LAB

Today we will go though a working example of how data sharing can allow you to make better decisions.  We will be going through how to create a policy simulator in order to estimate the impact of changing the cold weather payment policy.

##### Go to the Private Share area to access the private datasets that you will need.  The private share should now be available, if not - let us know!

Once you press **Get** to get the data, you will see a new database appear in your trial account.  It should look like this:

![alt text](image-1.png)



#### Creating the Streamlit app.


Run the SQL Script [run sql setup](sql_setup.sql)

Today we will manually add the files needed to run the app using the snowflake UI.  however, in practices it is much easier to leverage Visual Studio Code as demonstrated today.  You may wish to leverage VSCode in the second part of the hackathon.

You can import this SQL script and run all within Snow sight.


- Open up the **POLICY_CHANGE_SIMULATOR_STREAMLIT** database and navigate to the streamlit stage

![alt text](image-2.png)

- Download following file and import it to the the stage

[home.py](Home.py)

![alt text](image-3.png)

Download the following pages and add to a new directory called pages

![alt text](image-5.png)


[pages](https://github.com/sfc-gh-boconnor/government_hackathon/tree/main/pages)

![alt text](image-4.png)



Download the following images and add to a new directory called images

[Images](https://github.com/sfc-gh-boconnor/government_hackathon/tree/main/images)

![Images](image-6.png)

Download the following file and add it to the home directory

[enviroment.yml](https://github.com/sfc-gh-boconnor/government_hackathon/tree/main/environment.yml)

![alt text](image-8.png)

- In the Projects area, click on Streamlit

- You should ses a new streamlit app appear called **Policy Change Simulator**

![Image](image-7.png)

Click on the **new app** and wait for it to start.


##### Create a Scenario
Leave the settings 'as-is' in the sidebar, and give the scenario a name

Press Save Scenario for more details

![alt text](image-9.png)

You will see summary metrics based on live calculation - all by using shared datasets.

![alt text](image-10.png)

>**FACT**  You can create a packaged app which have all the dependent SQL, python packages, images and steamlits which are called 'Native apps'.  This makes a fully functioning app easy to distribute.



#### Data Analysis

We will use the built in notebooks to do some analysis on the synthetic data.  Before this, go to the market place and search for More Metrics.  We will add Residential postcodes as an additional dataset.  Do not change the name of the database.

![alt text](image-13.png)

#### Viewing the data with a notebook

Create a New Notebook 

![alt text](image-11.png)

Under packages, add matplotlib

![alt text](image-12.png)

Import the following libraries:

```python

#  Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

# Import python packages
import streamlit as st
import pandas as pd
from snowflake.snowpark import functions as F   
from snowflake.snowpark.window import Window
# We can also use Snowpark for our analyses!
from snowflake.snowpark.context import get_active_session
session = get_active_session()
from snowflake.snowpark import types as T


```
Our first part of the analysis is to look at the 'WHO'.  The provided shared dataset contains a synthetic population dataset.  We will have a look at the contents of this.

Copy and paste the following python code into a new cell:

```python
population = session.table('COLD_WEATHER_PAYMENTS_DATASET.DATA."Synthetic Population"')

col1,col2,col3,col4= st.columns(4)

with col1:
    st.metric('Total Population: ', population.count())
with col2:
    st.metric('Total Households:', population.select('HOUSEHOLD').distinct().count())
with col3:
    st.metric('Total Not Working', population.filter(F.col('OCCUPATION_CODE')==2).count())
with col4:
    st.metric('Total Under 16yr olds', population.filter(F.col('OCCUPATION_CODE')!=1).count())
```

You can also view the same information using SQL.

Copy and past the following into a new **SQL** cell:

```sql

SELECT COUNT(*) "Total People", APPROX_COUNT_DISTINCT(HOUSEHOLD) "Total Households", COUNT(CASE OCCUPATION_CODE WHEN 2 THEN 1 END) "Total Not Working" FROM COLD_WEATHER_PAYMENTS_DATASET.DATA."Synthetic Population"
```

Now lets look at a sample of the population.  We will look at a sample of 20% of the population and then limit the return to 100 rows

```python

population.sample(0.2).limit(100);

```

Lets see counts of the population py occupations and gender

```python

gender = population.group_by('SEX').count()
occupation = population.group_by('OCCUPATION').agg(F.any_value('OCCUPATION_CODE').alias('Occupation Code')
                                                   ,F.count('*').alias('COUNT'))

st.table(gender)
st.table(occupation)

```

We will utilise streamlit's basic charting capabilities to simply look at the distribution by occupation and gender

```python

st.markdown('People by Occupation and Sex')
col1, col2 = st.columns(2)
with col1:
    st.bar_chart(occupation,x='OCCUPATION',y='COUNT')
with col2:
    st.bar_chart(gender,x='SEX',y='COUNT')

```

We can use this information to filter the citizens

```python

col1,col2,col3 = st.columns(3)
with col1:
    Gender = st.radio('Gender',gender)
with col2:
    elderly = st.selectbox('Occupation',occupation)
with col3:
    Age_Range = st.slider('Age Range',1,99,(1,99))

```

Add a SQL sell which will reveal a sample of the sample population

```sql

select * from (select * from COLD_WEATHER_PAYMENTS_DATASET.DATA."Synthetic Population"  where SEX = '{{Gender}}' and AGE BETWEEN {{Age_Range[0]}}AND {{Age_Range[1]}} )sample(100 rows)

```

For the calculator, I have decided that all policies will be based around citizens who are not working, and live in households where everyone else is not working.

lets start of by creating a dataset based on people who are not working

```python

population_not_working = population.filter(F.col('OCCUPATION_CODE')==2)

population_not_working.limit(10)

```

We will now create a table which counts the number of people working in every household.

```python

population_working = population.filter((F.col('OCCUPATION_CODE')!=2) | (F.col('OCCUPATION_CODE')==1))

working_household = population_working.select('HOUSEHOLD','NI NUMBER').group_by(F.col('HOUSEHOLD')).agg(F.count('*').alias('WORKING_PEOPLE'))

working_household.limit(10)

```

Let's now visualise the people who are not working and also do not live with anyone who is working.  To do this we did a join to the the working household datafreame we just created and then filtered out any matches

```python

import matplotlib.pyplot as plt

population_entitled_cold_weather = population_not_working.join(working_household, on=(population_not_working['HOUSEHOLD']==working_household['HOUSEHOLD']), how='outer',rsuffix='_L').drop('HOUSEHOLD_L')\
.filter(F.col('WORKING_PEOPLE').isNull()).drop('WORKING_PEOPLE')

st.metric('Total entitled for cold weather payments:', population_entitled_cold_weather.count())

st.markdown('#### Sample of data extracted')
population_entitled_cold_weather.sample(0.5).limit(10)
hist = population_entitled_cold_weather.select(F.col('AGE')).distinct().to_pandas().hist(bins=7)

col1,col2,col3 = st.columns([0.2,0.6,0.2])
with col2:
    plt.show()

```

Now, let's create a table with names and addresses of all households who will get a cold weather payment if the weather permits this.

```python

households_cold_weather = population_entitled_cold_weather.with_column('ELECTRICITY_BILL_PAYER',F.concat('FIRST_NAME',F.lit(' '),'LAST_NAME')).group_by('HOUSEHOLD','ADDRESS_1','ADDRESS_2','ADDRESS_3','POSTCODE','LSOA_CODE')\
.agg(F.any_value('ELECTRICITY_BILL_PAYER').alias('HOUSEHOLD_BILL_PAYER'),F.count('*').alias('NUMBER OF OCCUPANTS'))

households_cold_weather.sample(0.2).limit(10)

```

We have now managed to work out who would be entitled based on who is not working, and who doesnt live with anyone who is working.  Of course, in reality the selection would be more scientific - such as measuring based on who is receiving universal credits.



TBA   .... this will be a notebook which will leverage the data in the private share.

#### Creating Your own private listing

TBA