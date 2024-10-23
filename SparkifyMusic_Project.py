#!/usr/bin/env python
# coding: utf-8
## PURPOSE
## To design an Apache Cassandra database for Sparkify’s music streaming app to model song play data, building an ETL pipeline in Python 
## to process user activity from CSV files and enable efficient querying of song listening behavior by doing the following:
## 1. Retrieve song data with a specific session id and item in session. 2. Retrieve song data for specific user and session. 3. Retrieve user names who listened to a specicfic song.

# # Part I. ETL Pipeline for Pre-Processing the Files

# ## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

# #### Import Python packages 


# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)


# #### Creating list of filepaths to process original event csv data files

# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables


# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# # Part II. Complete the Apache Cassandra coding portion of your project. 
# 
# ## Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId
# 
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# ## Begin writing your Apache Cassandra code in the cells below


# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()


# Create a Keyspace 
query = """
CREATE KEYSPACE IF NOT EXISTS music_history 
WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
"""
session.execute(query)

# Set KEYSPACE 
session.set_keyspace('music_history')


## Task 1: Retrieve song data with a specific session id and item in session. 
## Query gets the artist, song title and song's length in the music app history that was heard during 
## sessionId = 338, and itemInSession = 4

# create table with sessionID as partition key and itemInSession as clustering column
query = """
CREATE TABLE IF NOT EXISTS song_info_by_session(
sessionId int,
itemInSession int,
artist text,
song text, 
length float,
PRIMARY KEY (sessionId, itemInSession))
""" 
session.execute(query)



# insert data in table
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO song_info_by_session (sessionId, itemInSession, artist, song, length)"
        query = query + "VALUES (%s, %s, %s, %s, %s)"
        
        # execute query.
        session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))


# select data from table
query = "SELECT sessionId, itemInSession, artist, song, length from song_info_by_session WHERE sessionId = 338 AND itemInSession = 4;"

# execute query
rows = session.execute(query)

#store data in list
data = []
for row in rows:
    data.append([row.sessionid, row.iteminsession, row.artist, row.song, row.length])
    
# create dataframe 
df = pd.DataFrame(data, columns =['Session Id', 'Item in Session', 'Artist', 'Song', 'Length'])
    
# print dataframe 
print("Results for Session ID is 338 and Item in Session is 4:")
print(df)


## Task 2: Retrieve song data for specific user and session 
## Query retrives artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

# create song_info_by_user table with composite partition keys on sessionId and userId and clustering column on itemInSession 
query = """
CREATE TABLE IF NOT EXISTS song_info_by_user(
    userId int,
    sessionId int,
    artist text, 
    song text,  
    firstName text,
    lastName text,
    itemInSession int,
    PRIMARY KEY ((userId, sessionId), itemInSession))
    WITH CLUSTERING ORDER BY (itemInSession ASC)
"""
session.execute(query)

# insert data in table
file = 'event_datafile_new.csv'
with open(file, encoding='utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)  # Skip header
    for line in csvreader:
        query = "INSERT INTO song_info_by_user (userId, sessionId, itemInsession, artist, song, firstName, lastName) "
        query += "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))

# select data from table       
select_query = """
SELECT artist, song, firstName, lastName, itemInSession 
FROM song_info_by_user 
WHERE userId = 10 AND sessionId = 182 
ORDER BY itemInSession ASC;
"""

# execute the query
rows = session.execute(select_query)

# store data in a list
data = []
for row in rows:
    data.append([row.artist, row.song, row.iteminsession, row.firstname, row.lastname])

# create dataframe
df = pd.DataFrame(data, columns =['Artist', 'Song', 'Item in Session', 'First Name', 'Last Name'])

# print dataframe
print("User ID is 10 and Session ID is 182:")
print(df)


# In[40]:


## Task 3: Retrieve user names who listened to a specicfic song
# Query retrives all users first and last who have listened to the song "All Hands Against His Own"

# create user_info_by_song table with a partition key on song and clustering column on userId
query = """
CREATE TABLE IF NOT EXISTS user_info_by_song (
    song text,
    userId int,
    firstName text,
    lastName text,
    PRIMARY KEY (song, userId))
"""
session.execute(query)

# insert data into table 
with open(file, encoding='utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)  # Skip header
    for line in csvreader:
        query = "INSERT INTO user_info_by_song (song, userId, firstName, lastName)"
        query = query + " VALUES (%s, %s, %s, %s)"
        session.execute(query, (line[9], int(line[10]), line[1], line[4]))
        
# select data from table for the song 'All Hands Against His Own'           
select_query = """
SELECT song, userId, firstName, lastName
FROM user_info_by_song
WHERE song = 'All Hands Against His Own'
ALLOW FILTERING;
"""

# execute the query
rows = session.execute(select_query)

#store data in a list
data = []
for row in rows:
    data.append([row.song, row.userid, row.firstname, row.lastname]) 

# create data frame   
df = pd.DataFrame(data, columns=['Song', 'User ID', 'First Name', 'Last Name'])

# print dataframe
print("Users who listened to the song 'All Hands Against His Own':")
print(df)



# ### Drop the tables before closing out the sessions

# In[41]:


#drop the table before closing out the sessions


# In[42]:


session.execute("DROP TABLE IF EXISTS song_info_by_session")
session.execute("DROP TABLE IF EXISTS song_info_by_user")
session.execute("DROP TABLE IF EXISTS user_info_by_song")


# ### Close the session and cluster connection¶

# In[43]:


session.shutdown()
cluster.shutdown()





