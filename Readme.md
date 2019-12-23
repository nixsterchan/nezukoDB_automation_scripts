# Welcome to NezukoDB automation!
 **50.043 Database GreatReads project 2019**

Group members:
-  Abinaya
- Bertha Han
- Kenneth Soon
- Nigel Chan
- Sumedha Gn
- Tamanna 
![Image result for nezuko e](https://66.media.tumblr.com/2f02891eac278179c9812116a0a266be/3805e63377d89112-29/s640x960/60272d35ed9c61f889237de6f65bb1934a40eabd.jpg)

## 1. Getting the Dependencies
> requiremetns.txt .

Make sure you are in the project **root** folder. 
- Start with creating a virtualenv (e.g. myvenv), download if haven't
- `source ./myvenv/bin/activate`
- `pip3 install -r requirements.txt`
- If `requirements.txt` fails to execute, install `boto3` and `fabric` python lib packages
 

## 2. Setting up Production
> production.py .

Access into `cd dbds_project/Database/flaskproject` from your command line, run:
- `python3 app.py`
- Fill in the necessary informations into the respective prompts:
- < image of promps here>
	-  **AWS credentials**
	- **New or non-existing security group name**
	- **New or non-existing key name**
- Program will start running to set up mongoDB, MySQL and the Web Server instances
- Once the installation has been completed, a link to our web server will appear.
- To view the **functionalities of our Web Server**, here is the [link](https://github.com/hello2508/Database.git)

## 3. Setting up Analytics 
> analytics_functions.py .

Next, in your command line run:
- `python3 hadoop_spark_setup.py`
- The command line will prompt you to enter the necessary informations:
	- **Number of instances to create**
	- **Choose settings for AMI and instance type (default: Ubuntu 18.04)**
- The command line will execute the relevant scripts and will take approximately 27 mins to complete (might wanna do something for the time being).
Once the script is completed,  you can visit the links shown in the terminal to check if it was successful.

## 4. Getting the Analytics
> get_analytics.py . 

To get the Pearson Correlation and TF-IDF results, run:
- `python3 get_analytics.py`
- The server will start the task and results are generated in approx. 10mins. TF-IDF will be returned in `results.txt`file in a local repo and Pearson will be printed.

