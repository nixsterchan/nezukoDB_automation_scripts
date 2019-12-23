import analytics_functions
import boto3
from fabric import Connection
import os
import time

### This file will be run to send commands to the namenode to run the tfidf.py and pearson.py scripts

fil = open('./analytics_generated_items/namenode_ip_and_key', 'r')
namenode_ip_and_key = fil.read()
fil.close()

namenode_ip, key_pair = namenode_ip_and_key.split('\n')
### Get connected to namenode and start running commands ###

input('Press Enterget Pearson Correlation output score(wait around a minute or so): ')
c = analytics_functions.theconnector(namenode_ip, key_pair)
c.run('cd spark_scripts && python3 pearson.py')

# input('Press Enter to run the TFIDF script, results will arrive shortly in a file named tfidf_results')
# c.run('cd spark_scripts && python3 tfidf.py')
