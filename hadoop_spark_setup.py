#!/usr/bin/env python
# coding: utf-8

import boto3
from fabric import Connection
import os
import time
from botocore.exceptions import ClientError
import analytics_functions

# just to note time steps
start_time = time.time()
print("start time:")
print("--- %s seconds ---" % (time.time() - start_time))


### Set up your ec2 credentials with user input ###

# If credentials are invalid, loop until valid
valid = False

while(not valid):
  print('Good day sir, please enter your AWS credentials to proceed')
  
  aws_key = input('Please key in your AWS access key ID: ')
  
  aws_secret_key = input('Please key in your AWS secret access key: ')

  region = input('Please key in your current region: ')
  
  ec2 = boto3.client('ec2', aws_access_key_id=aws_key, aws_secret_access_key=aws_secret_key, region_name=region)
  
  # Test if the credentials were valid 
  try:
    analytics_functions.list_ec2_instances(ec2)
    # Turns true if no error
    valid = True
  except:
    print("The credentials were not valid.")


### Create your security group ###

# First set the ip permissions
permissions = [{'IpProtocol': 'tcp',
                   'FromPort': 22,
                   'ToPort': 22,
                   'IpRanges': [{
                       'CidrIp': '0.0.0.0/0',
                       'Description': 'SSH'}]},
                  {'IpProtocol': '-1',
                   'IpRanges': [{
                       'CidrIp': '0.0.0.0/0',
                       'Description': 'All'}]},
                  {'IpProtocol': 'tcp',
                   'FromPort': 3000,
                   'ToPort': 3000,
                   'IpRanges': [{
                       'CidrIp': '0.0.0.0/0',
                       'Description': 'Hadoop Clusters'}]}]

# Check to see if security group already exists
valid = False

while(not valid):
  security_group_name = input('Please enter desired name for your new security group. Ensure that the name has not been used for another group: ')
  security_group_description = input('Please enter desired description for your new security group: ')

  try:
    # try creating the group, if error, redo
    security_group_id = analytics_functions.create_security_group(security_group_name, security_group_description, permissions, ec2)
    valid = True
  except:
    print('It seems like the name is already taken, try another')

### Create your key pair into local directory for SSH purposes ###
valid = False

while(not valid):
  try:
    key_pair = input('Please enter your desired new key name that does not already exist: ')
    # test to see if this key name already exists
    analytics_functions.create_key_pair(key_pair, ec2)
    valid = True
  except:
    print('That key already exists, please give another name')


### Creating the EC2 instances ###

# First check to see if he wants to add in his own settings or use the default
valid = False

while(not valid):
  # Get number of nodes wanted
  specified_num_nodes = int(input('Enter number of instances you want created(inclusive of namenode): '))

  default = True

  while(default):
    yes_no = input('Would you like to use the default settings(m5ad.4xlarge and ubuntu18.04) for AMI type and instance type? [y/n]: ')

    if yes_no.lower() == 'y':
      # ec2_instance_size = 't2.medium'
      ec2_instance_size = 'm5ad.4xlarge'
      ami_type = 'ami-061eb2b23f9f8839c'
      default = False
    elif yes_no.lower() == 'n':
      ec2_instance_size = input('Enter your desired instance type eg(t2.micro): ')
      ami_type = input('Enter your desired AMI: ')
      default = False
    else:
      print('Please type y or n')

  # test to see if anything went wrong
  try:
    # this line will return a bunch of node ids for later use
    instance_node_list = analytics_functions.create_instances(ami_type, specified_num_nodes, ec2_instance_size, key_pair, security_group_id, ec2)
    valid = True
  except:
    print('An error occured, please check if defaults were correctly input')

### Get the list of ids for the namenode and datanodes ###

# set first variable as name node
namenode_id = instance_node_list[0]

# set others as datanodes
datanode_ids = instance_node_list[1:]

# get the public IP and DNS of each instance
time.sleep(3)
instance_dic = analytics_functions.list_ec2_instances(ec2)
dns_dic = analytics_functions.get_publicdns(ec2)

# all ip addresses
allnodes_ips = []
allnodes_dns = []
for i in instance_node_list:
    allnodes_ips.append(instance_dic[i])
    allnodes_dns.append(dns_dic[i]) 
    
print(allnodes_dns)

# namenode ip address
namenode_ip = instance_dic[namenode_id]
namenode_dns = dns_dic[namenode_id]

print(namenode_dns)

# datanodes ip addresses
datanode_ips = []
datanode_dns = []
for i in datanode_ids:
    datanode_ips.append(instance_dic[i])
    datanode_dns.append(dns_dic[i])
    
print(datanode_dns)


# # Using Fabric Connection function to run commands on the EC2 instance

# wait 60s for instances to start up
print("Waiting for instance to start up")
time.sleep(80)

print("time elapsed after start up of instances:")
print("--- %s seconds ---" % (time.time() - start_time))


# ## FOR ALL INSTANCES, WE UPDATE THE PACKAGES
for instance_ip in allnodes_ips:
  success = False
  tryfactor = 0
  while(not success):
    try: 
      c = analytics_functions.theconnector(instance_ip, key_pair)
      c.sudo('apt-get -y update')
      success = True

    except: 
      # in case fail
      print('something went wrong, retrying in a moment')
      tryfactor += 1
      if tryfactor == 10:
        print(f'It has been {tryfactor} times, something went horribly wrong. Ctrl C to exit and try again')
      time.sleep(10)
  

### After the first round of installation, we reboot ALL instances ###

# we reboot all the instances by passing in the list of all the instances' ids into the reboot sequence

try:
  ec2.reboot_instances(InstanceIds=instance_node_list, DryRun=True)
except ClientError as e:
  if 'DryRunOperation' not in str(e):
      print("You don't have permission to reboot instances.")
      raise

try:
  response = ec2.reboot_instances(InstanceIds=instance_node_list, DryRun=False)
  print('Success', response)
except ClientError as e:
  print('Error', e)

# Wait for instances to reboot for 60s
print("Waiting for Reboot")
time.sleep(60)


### NOW WE INSTALL JAVA ON ALL NODES ###

for instance_ip in allnodes_ips:
  success = False
  while(not success):
    try:
      c = analytics_functions.theconnector(instance_ip, key_pair)

      # Install java on the node
      c.sudo('apt-get -y install openjdk-8-jdk-headless')

      # Install Apache Hadoop 3.1.2 on the instances
      c.run('mkdir server')
      c.run('cd server && wget http://archive.apache.org/dist/hadoop/common/hadoop-3.1.2/hadoop-3.1.2.tar.gz')
      # c.run('wget http://archive.apache.org/dist/hadoop/common/hadoop-3.1.2/hadoop-3.1.2.tar.gz')
      c.run('cd server && tar xvzf hadoop-3.1.2.tar.gz')
      success = True
    except:
      print('something went wrong, sleeping for a bit before retrying')
      time.sleep(20)
    
print("time elapsed after java installs:")
print("--- %s seconds ---" % (time.time() - start_time))

### FOR ALL NODES, SET UP JAVAHOME PROPERLY ###

for instance_ip in allnodes_ips:
    c = analytics_functions.theconnector(instance_ip, key_pair)
    c.put('./allnodes-stuff/hadoop-env.sh')
    c.run('mv hadoop-env.sh ~/server/hadoop-3.1.2/etc/hadoop/')

### Add in our namenode DNS into core-site.xml and put to all nodes

# edit core site xml
template = './allnodes-stuff/core-site-template.xml'
output_file = './analytics_generated_items/core-site.xml'
term_to_change = '<nnode>'
new_term = namenode_dns

analytics_functions.replace_single_term(template, output_file, term_to_change, new_term)

# now to put into all instances
for instance_ip in allnodes_ips:
  c = analytics_functions.theconnector(instance_ip, key_pair)

  c.put('./analytics_generated_items/core-site.xml')
  c.run('mv core-site.xml ~/server/hadoop-3.1.2/etc/hadoop/')
  
  # create data directory on all the nodes and change ownership to ubuntu
  c.sudo('mkdir -p /usr/local/hadoop/hdfs/data')
  c.sudo('chown -R ubuntu:ubuntu /usr/local/hadoop/hdfs/data')
    
### Configuring Namenode for password-less SSH to datanodes ###
#First start by creating an SSH key pair in our namenode

success = False
while(not success):
  try:
    ## Connect to Namenode and generate key
    c = analytics_functions.theconnector(namenode_ip, key_pair)

    # gen keys
    c.run('ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa')
    success = True
  except:
    # in case fail
    print('something went wrong, sleeping for a bit before retrying')
    time.sleep(20)

# get the public key from namenode
c.get('.ssh/id_rsa.pub')

# # Using the Namenode's key, put it up into the Datanodes and cat to authorized
for instance_ip in datanode_ips:
  success = False
  while(not success):
    try:
      c = analytics_functions.theconnector(instance_ip, key_pair)

      c.put('id_rsa.pub')
      c.run('cat id_rsa.pub >> ~/.ssh/authorized_keys')
      success = True
    except:
      print('something went wrong, sleeping for a bit before retrying')
      time.sleep(20)

# We cat into the namenode's authorized keys as well
success = False
while(not success):
  try:
    # cat to namenode authorized keys as well
    c = analytics_functions.theconnector(namenode_ip, key_pair)
    c.run('cd .ssh && cat id_rsa.pub >> ~/.ssh/authorized_keys')
    success = True
  except:
    print('something went wrong, sleeping for a bit before retrying')
    time.sleep(20)

### Configure config file for .ssh in NameNode ###

# Create config file using DNSes of all nodes 
config = open('./analytics_generated_items/config', 'w')
node_host_names = []

for index, dns in enumerate(allnodes_dns):
  if index == 0:
      node_name = 'nnode'
  else:
      node_name = 'dnode' + str(index)
      
  node_host_names.append(node_name)
  config.write(f'Host {node_name} \n')
  config.write(f'  HostName {dns} \n')
  config.write(f'  User ubuntu \n')
  config.write(f'  IdentityFile ~/.ssh/id_rsa \n')
  config.write(f'\n')
    
config.close()
    
### Place config file in the namenode .ssh folder ###

# Connect to Namenode and set config file into .ssh folder
success = False
while(not success):
  try:
    c = analytics_functions.theconnector(namenode_ip, key_pair)

    c.put('./analytics_generated_items/config')
    c.run('mv config ~/.ssh')
    c.run('chmod 600 ~/.ssh/config') # run this to give permission
    time.sleep(2)
    success = True
  except:
    print('something went wrong, sleeping for a bit before retrying')
    time.sleep(20)

# ### Now from Namenode, ssh into the other nodes

# loop through node indexes and ssh and exit each
for index in range(1, len(allnodes_ips)):
#   if index == 0:
#     node = 'nnode'
#   else:
  node = 'dnode' + str(index)
      
  success = False
  while(not success):
    try:
      c = analytics_functions.theconnector(namenode_ip, key_pair)
      # ssh into nodes and bypass question
      c.run(f'cat ~/.ssh/id_rsa.pub | ssh -o StrictHostKeyChecking=no {node}' +' "cat >> ~/.ssh/authorized_keys"')
      # exit the node
      c.close()
      print(index)
      success = True
    except:
      print('something went wrong, sleeping for a bit before retrying')
      time.sleep(20)

### Setting up HDFS properties on Namenode ###

# For Namenode, edit hdfs-site.xml file
c = analytics_functions.theconnector(namenode_ip, key_pair)

# replace hdfssite file in namenode
c.put('./namenode-stuff/hdfs-site.xml')
c.run('mv hdfs-site.xml ~/server/hadoop-3.1.2/etc/hadoop/')

### Setting up MapReduce properties on Namenode

# Edit our mapred-site.xml
template = './namenode-stuff/mapred-site-template.xml'
output_file = './analytics_generated_items/mapred-site.xml'
term_to_change = '<nnode>'
new_term = namenode_dns

analytics_functions.replace_single_term(template, output_file, term_to_change, new_term)

# Now put the file into our namenode's folder
c.put('./analytics_generated_items/mapred-site.xml')
c.run('mv mapred-site.xml ~/server/hadoop-3.1.2/etc/hadoop/')

### Set up YARN properties on Namenode ###

# Edit our yarn-site.xml file
template = './namenode-stuff/yarn-site-template.xml'
output_file = './analytics_generated_items/yarn-site.xml'
term_to_change = '<nnode>'
new_term = namenode_dns

analytics_functions.replace_single_term(template, output_file, term_to_change, new_term)

# Put it into namenode's folder
c.put('./analytics_generated_items/yarn-site.xml')
c.run('mv yarn-site.xml ~/server/hadoop-3.1.2/etc/hadoop/')

### Set up Master and Worker Files for Namenode

# Create masters file and add namenode dns
master = open('./analytics_generated_items/masters', 'w')
master.write(f'{namenode_dns}')
master.close()

# Create workers file and add datanode dnses
workers = open('./analytics_generated_items/workers', 'w')
for dns in datanode_dns:
  workers.write(f'{dns}\n')
workers.close()

# Put the files up to namenode
c.put('./analytics_generated_items/masters')
c.run('mv masters ~/server/hadoop-3.1.2/etc/hadoop/')
c.put('./analytics_generated_items/workers')
c.run('mv workers ~/server/hadoop-3.1.2/etc/hadoop/')

### Replace hdfs.xml files in all datanodes ###
for instance_ip in datanode_ips:
  c = analytics_functions.theconnector(instance_ip, key_pair)
  c.put('./datanodes-stuff/hdfs-site.xml')
  c.run('mv hdfs-site.xml ~/server/hadoop-3.1.2/etc/hadoop/')

### Now to test starting up the hadoop cluster ###

# First format namdenode
c = analytics_functions.theconnector(namenode_ip, key_pair)
c.run('~/server/hadoop-3.1.2/bin/hdfs namenode -format')

# Now start the cluster!
c.run('~/server/hadoop-3.1.2/sbin/start-dfs.sh')

print("time elapsed after running hadoop cluster:")
print("--- %s seconds ---" % (time.time() - start_time))

# Short delay while hadoop starts up to avoid any issues
time.sleep(30)


# ### Set up Spark on namenode
# success = False
# while(not success):
#   try:
#     c = analytics_functions.theconnector(namenode_ip, key_pair)

#     # Get spark first
#     c.run('mkdir spark')
#     c.run('cd spark && wget https://www-eu.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz')
#     c.run('cd spark && tar zxvf spark-2.4.4-bin-hadoop2.7.tgz')

#     # Now update the bashrc files
#     c.put('./allnodes-stuff/spark-cat')
#     c.run('cat spark-cat >> ~/.bashrc')
#     success = True
#   except:
#     print("something went wrong, sleeping for a bit before retrying")
#     time.sleep(20)

### Set up Spark on namenode
for instance_ip in allnodes_ips:
  success = False
  while(not success):
    try:
      c = analytics_functions.theconnector(instance_ip, key_pair)

      # Get spark first
      c.run('mkdir spark')
      c.run('cd spark && wget https://www-eu.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz')
      c.run('cd spark && tar zxvf spark-2.4.4-bin-hadoop2.7.tgz')

      # Now update the bashrc files
      c.put('./allnodes-stuff/spark-cat')
      c.run('cat spark-cat >> ~/.bashrc')
      success = True
    except:
      print("something went wrong, sleeping for a bit before retrying")
      time.sleep(20)
    
print("time elapsed after setting up spark on all nodes:")
print("--- %s seconds ---" % (time.time() - start_time))

### Set up slave file on namenode spark ###
slaves = open('./analytics_generated_items/slaves', 'w')
for dns in datanode_dns:
    slaves.write(f'{dns}\n')
slaves.close()

# Now add slaves file to namenode
c = analytics_functions.theconnector(namenode_ip, key_pair)

c.put('./analytics_generated_items/slaves')
c.run('mv slaves ~/spark/spark-2.4.4-bin-hadoop2.7/conf/')

### RUN THUNDERSHOCKKKKKK ###
c.run('~/spark/spark-2.4.4-bin-hadoop2.7/sbin/start-all.sh')

### Next we get the csv files needed into our cluster ###
# metadata.csv
success = False
while(not success):
  try: 
    c = analytics_functions.theconnector(namenode_ip, key_pair)
    c.run('wget https://dbds-kindle-reviews.s3-ap-southeast-1.amazonaws.com/metadata.csv')
    c.run('./server/hadoop-3.1.2/bin/hdfs dfs -put metadata.csv /')
    success = True
  except:
    print('something went wrong, sleepin for a bit')
    c.run('rm metadata.csv')
    c.close()
    time.sleep(20)

# kindle_reviews.csv
success = False
while(not success):
  try: 
    c = analytics_functions.theconnector(namenode_ip, key_pair)
    c.run('wget https://dbds-kindle-reviews.s3-ap-southeast-1.amazonaws.com/kindle_reviews.csv')
    c.run('./server/hadoop-3.1.2/bin/hdfs dfs -put kindle_reviews.csv /')
    success = True
  except:
    print('something went wrong, sleepin for a bit')
    c.run('rm kindle_reviews.csv')
    c.close()
    time.sleep(20)

# Just for checking
c.run('./server/hadoop-3.1.2/bin/hdfs dfs -ls /')

print(f"Total Time elapsed to run {specified_num_nodes} nodes in the cluster:")
print("--- %s seconds ---" % (time.time() - start_time))


# write the dns into a file for use in pyspark later
fil = open('./analytics_generated_items/namenode_url', 'w')
fil.write(f'hdfs://{namenode_dns}:9000/')
fil.close()

# write the ip and key name into a file for use in getting analytics later
fil2 = open('./analytics_generated_items/namenode_ip_and_key', 'w')
fil2.write(f'{namenode_ip}\n')
fil2.write(f'{key_pair}')
fil2.close()


### install some dependencies on namenode
success = False
while(not success):
  try: 
    c = analytics_functions.theconnector(namenode_ip, key_pair)
    c.sudo('apt -y install python3-pip')
    c.run('pip3 install numpy pyspark')

    success = True
  except:
    print('something went wrong, sleepin for a bit')
    c.close()
    time.sleep(20)

### now to push up our spark scripts into a designated spark folder in namenode
success = False
while(not success):
  try: 
    c = analytics_functions.theconnector(namenode_ip, key_pair)
    c.put('./analytics_generated_items/namenode_url')
    c.put('./sparky/pearson.py')
    c.put('./sparky/tfidf.py')

    c.run('mkdir spark_scripts && mv namenode_url pearson.py tfidf.py ~/spark_scripts')

    success = True
  except:
    print('something went wrong, sleepin for a bit')
    c.close()
    time.sleep(20)


  
print(f"DONEEE. TOTAL RUN TIME WAS:")
print("--- %s seconds ---" % (time.time() - start_time))