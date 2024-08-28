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



- In the Projects area, click on Streamlit

- You Should se a new streamlit app appear called **Policy Change Simulator**
![Image](image-7.png)

Click on the new app and wait for it to start.

#### Viewing the data with a notebook



TBA   .... this will be a notebook which will leverage the data in the private share.

#### Creating Your own private listing

TBA