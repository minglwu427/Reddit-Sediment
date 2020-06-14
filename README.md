# Reddit-Sediment
Reddit Sediment Project for School

This file contains three files and we will .  The actual data is too large to be included in the GitHub.  However, it is roughly over 10GB of reddit comments in the politic section during 2016.  We use these data to predict Donald Trump approval rate through out the election.  The goal of this project is to allows student like myself to get a frist hand experience with Apache Spark, a cluster-computing framework. 

The the three files contains

1. cleantext.py	- This script cleans the comments and separate sentence into token so we can use bag of words to analyze the sentiment.   It also removes all the punctions during the process.  
2. p2b_plots.py - This script is used to graph the the approval rate on the US map
3. reddit_model.py - This is the main script that shows how I utilized Apache Spark to train the Logisitic Regression of the model and how I use it for the predicition. 
