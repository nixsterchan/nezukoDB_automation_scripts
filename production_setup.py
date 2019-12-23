#!/usr/bin/env python
# coding: utf-8

import boto3
import os
import time
from fabric import Connection
from botocore.exceptions import ClientError
import analytics_functions

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

### We will have 3 different Security groups for each Instance ###

## MongoDB Permissions
mongo_permissions = [{'IpProtocol': 'tcp',
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
                   'FromPort': 27017,
                   'ToPort': 27017,
                   'IpRanges': [{
                       'CidrIp':'0.0.0.0/0',
                       'Description': 'MongoDB'}]}]
# ### Mysql Permissions
mysql_permissions = [{'IpProtocol': 'tcp',
                   'FromPort': 22,
                   'ToPort': 22,
                   'IpRanges': [{
                       'CidrIp': '0.0.0.0/0',
                       'Description': 'SSH'}]},
                  {'IpProtocol': 'tcp',
                   'FromPort': 80,
                   'ToPort': 80,
                   'IpRanges': [{
                       'CidrIp': '0.0.0.0/0',
                       'Description': 'HTTP'}]},
                  {'IpProtocol': 'tcp',
                   'FromPort': 3306,
                   'ToPort': 3306,
                   'IpRanges': [{
                       'CidrIp': '0.0.0.0/0',
                       'Description': 'MySQL'}]}]
# ### Web Server Permissions
web_permissions = [{'IpProtocol': 'tcp',
                   'FromPort': 22,
                   'ToPort': 22,
                   'IpRanges': [{
                       'CidrIp': '0.0.0.0/0',
                       'Description': 'SSH'}]},
                  {'IpProtocol': 'tcp',
                   'FromPort': 80,
                   'ToPort': 80,
                   'IpRanges': [{
                       'CidrIp': '0.0.0.0/0',
                       'Description': 'HTTP'}]}]
                       
# ### Create the respective security groups 
# Check to see if security group already exists

### Mongo Security Group
valid = False
while (not valid):
    mon_security_group_name = input('Please enter a unique name for your MongoDB security group: ')
    mon_des = input('Please enter desired description for your new MongoDB security group: ')

    try:
        # try creating the group, if error, redo
        mon_secure = analytics_functions.create_security_group(mon_security_group_name, mon_des, mongo_permissions, ec2)
        valid = True
    except:
        print('It seems like the name is already taken, try another')

### Mysql Security Group
valid = False
while (not valid):
    mys_security_group_name = input('Please enter a unique name for your Mysql security group: ')
    mys_des = input('Please enter desired description for your new Mysql security group: ')

    try:
        # try creating the group, if error, redo
        mys_secure = analytics_functions.create_security_group(mys_security_group_name, mys_des, mysql_permissions, ec2)
        valid = True
    except:
        print('It seems like the name is already taken, try another')

### Web Server Security Group
valid = False
while (not valid):
    web_security_group_name = input('Please enter a unique name for your Web Server security group: ')
    web_des = input('Please enter desired description for your new Web Server security group: ')

    try:
        # try creating the group, if error, redo
        web_secure = analytics_functions.create_security_group(web_security_group_name, web_des, web_permissions, ec2)
        valid = True
    except:
        print('It seems like the name is already taken, try another')

### Create your key pair into local directory for SSH purposes ###
valid = False
while (not valid):
    try:
        key_pair = input('Please enter your desired new key name that does not already exist: ')
        # test to see if this key name already exists
        analytics_functions.create_key_pair(key_pair, ec2)
        valid = True
    except:
        print('That key already exists, please give another name')

### Get AMI and Instance Types
valid = False
while(not valid):
    default = True
    while(default):
        yes_no = input(
            'Would you like to use the default settings(t3a.2xlarge and ubuntu18.04) for AMI type and instance type? [y/n]: ')

        if yes_no.lower() == 'y':
            # tier = 't2.medium'
            tier = 't3a.2xlarge'
            instance_ami = 'ami-061eb2b23f9f8839c'
            default = False
        elif yes_no.lower() == 'n':
            tier = input('Enter your desired instance type eg(t2.micro): ')
            instance_ami = input('Enter your desired AMI: ')
            default = False
        else:
            print('Please type y or n')

    # test to see if anything went wrong
    try:
        ### Mongo Instance
        mongo_instance = analytics_functions.create_instances(instance_ami, 1, tier, key_pair, mon_secure, ec2)
        mongo_node_id = mongo_instance[0]
        mongo_node_id
        valid = True
    except:
        print('An error occured, please check if defaults were incorrectly input')

### Create rest of the instances
### Mysql Instance
mysql_instance = analytics_functions.create_instances(instance_ami, 1, tier, key_pair, mys_secure, ec2)
mysql_node_id = mysql_instance[0]
mysql_node_id

### Web Server Instance
web_instance = analytics_functions.create_instances(instance_ami, 1, tier, key_pair, web_secure, ec2)
web_node_id = web_instance[0]
web_node_id

# A short delay in case
time.sleep(3)

# ## Now to get the IPs and DNSes of each Instance
# Get the 2 dictionaries
instance_dic = analytics_functions.list_ec2_instances(ec2)
dns_dic = analytics_functions.get_publicdns(ec2)

# Mongo IP address and DNS
mongo_ip = instance_dic[mongo_node_id]
print('mongo ip\n', mongo_ip)
mongo_dns = dns_dic[mongo_node_id]
print('mongo dns\n', mongo_dns)

# Mysql IP address and DNS
mysql_ip = instance_dic[mysql_node_id]
print('mysql ip\n', mysql_ip)
mysql_dns = dns_dic[mysql_node_id]
print('mysql dns\n', mysql_dns)

# Web Server IP address and DNS
web_ip = instance_dic[web_node_id]
print('web ip\n', web_ip)
web_dns = dns_dic[web_node_id]
print('web dns\n', web_dns)

### For easy access to all IPs
all_node_ips = [mongo_ip, mysql_ip, web_ip]
all_node_ids = [mongo_node_id, mysql_node_id, web_node_id]


# wait 60s for instances to start up
print("Waiting for instance to start up")
time.sleep(60)

print("time elapsed after start up of instances:")
print("--- %s seconds ---" % (time.time() - start_time))


# ## Update the packages for ALL instances
for instance_ip in all_node_ips:
    success = False
    while(not success):
        try: 
            c = analytics_functions.theconnector(instance_ip, key_pair)
            c.sudo('apt-get update')
            success = True

        except: 
            # in case fail
            print('something went wrong, retrying i a moment')
            time.sleep(10)

# ## Do a reboot after that 

# we reboot all the instances by passing in the list of all the instances' ids into the reboot sequence

try:
    ec2.reboot_instances(InstanceIds=all_node_ids, DryRun=True)
except ClientError as e:
    if 'DryRunOperation' not in str(e):
        print("You don't have permission to reboot instances.")
        raise

try:
    response = ec2.reboot_instances(InstanceIds=all_node_ids, DryRun=False)
    print('Success', response)
except ClientError as e:
    print('Error', e)

# Wait for instances to reboot for 60s
print("Waiting for Reboot")
time.sleep(60)

# write to your .env file (which will be used for our web server)
name_of_db = 'nezukodb'
mongo_url = f'mongodb://{mongo_ip}' # insert mongo url here
environment_file = open(".env", 'w')
environment_file.write(f'database_name={name_of_db}\n')
environment_file.write(f'mongo_url={mongo_url}\n')
environment_file.write(f'host={mysql_ip}')

environment_file.close()

# # Prepare the MongoDB instance
success = False
while(not success):
    try: 
        # start a connection to MongoDB
        c = analytics_functions.theconnector(mongo_ip, key_pair)

        # get all necessary stuffs
        c.sudo('apt-get install gnupg')
        c.run('wget -qO - https://www.mongodb.org/static/pgp/server-4.2.asc | sudo apt-key add -')
        c.run('echo "deb [ arch=amd64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.2 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.2.list')
        c.sudo('apt-get update')
        c.sudo('apt-get install -y mongodb-org')
        c.sudo('service mongod start')

        c.run('wget https://dbds-kindle-reviews.s3-ap-southeast-1.amazonaws.com/metadata.bson')
        c.run(f'mongorestore --db {name_of_db} metadata.bson')
        c.sudo("sed -i 's/127.0.0.1/0.0.0.0/g' /etc/mongod.conf")
        c.sudo('service mongod restart')
        success = True

    except: 
        # in case fail
        print('something went wrong, retrying i a moment')
        time.sleep(10)

# # Prepare the Mqsql instance
success = False
while(not success):
    try: 
        c = analytics_functions.theconnector(mysql_ip, key_pair)
        ### Install the mysql
        c.sudo('apt-get -y install mysql-server')
        
        # Transfer kindle_reviews.csv from S3 to the instance
        c.run('mkdir data') 
        c.run('cd data && wget -c https://dbds-kindle-reviews.s3-ap-southeast-1.amazonaws.com/kindlereviews.sql')
        
        # Setting up kindle_reviews database
        c.sudo('mysql -e "create database dbds"')
        c.sudo('mysql -e "' + 'GRANT ALL PRIVILEGES ON *.* TO' + "'ubuntu'" + 'IDENTIFIED BY' + "'password';" + '"')
        c.run('cd data && mysql -u ubuntu -ppassword -D dbds -e "source kindlereviews.sql"')
        c.sudo("sed -i 's/bind-address/#bind-address/g' /etc/mysql/mysql.conf.d/mysqld.cnf")
        c.sudo('service mysql restart')

        success = True

    except: 
        # in case fail
        print('something went wrong, retrying i a moment')
        time.sleep(10)

# # Prepare the Web Server instance
success = False
while(not success):
    try:
        # Make the directionary
        c = analytics_functions.theconnector(web_ip, key_pair)
        c.run('git clone https://github.com/hello2508/Database.git')
        # sudo apt-get update
        c.sudo('apt -y install python3-pip')
        c.put('.env')
        c.run('mv .env ./Database/flaskproject')
        # c.sudo('apt install -y python3-venv')
        # c.run('cd Database/flaskproject/ && python3 -m venv webenv')
        # c.run('cd Database/flaskproject/ && source webenv/bin/activate')

        c.run('pip3 install --upgrade setuptools')
        c.run('pip3 install mysql-connector')
        c.run('pip3 install flask')
        c.run('pip3 install -U python-dotenv')
        c.run('pip3 install pymongo')
        c.run('pip3 install numpy')
        success = True
    except:
        # in case fail
        print('something went wrong, retrying i a moment')
        time.sleep(10)


try:
    c.run('cd Database/flaskproject && python3 app.py')

except ValueError:
    pass

print('All Done Setting Up The Production Site, You Can Access It By Using The Following Link: ')
print('127.0.0.1:5000')