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
> requirements.txt .

Make sure you are in the project **root** folder. 
- For this project, installation of `boto3` and `fabric` python lib packages are required
- `pip3 install boto3`
- `pip3 install fabric`
 

## 2. Setting up Production
> production_setup.py .

From the root folder, from your command line, run:
- `python3 production_setup.py`
- A series of prompts will greet you:
- < image of promps here>
	-  **AWS credentials**
	- **Non-existing security group names for three instances**
	- **Non-existing key name**
- Program will start running to set up mongoDB, MySQL and the Web Server instances
- Once the installation has been completed, a link to our web server will appear.
- To view the **functionalities of our Web Server**, here is the [link](https://github.com/hello2508/Database.git)

## 3. Setting up Analytics 
> analytics_functions.py .

For this section, once again type the following into your command line and run:
- `python3 hadoop_spark_setup.py`
- Similar to before, the command line will prompt you to enter the necessary informations:
	- **Number of instances to create**
	- **Choice of either choosing your own settings for AMI and instance type or using default (default: Ubuntu 18.04)**
- The command line will execute the relevant scripts and will take approximately 27 mins to complete (might wanna do something for the time being).
Once the script is completed, you can visit the links shown in the terminal to check out the hadoop and spark UIs to see if it was successful.

## 4. Getting the Analytics
> get_analytics.py . 

To get the Pearson Correlation and TF-IDF results, run:
- `python3 get_analytics.py`
- The server will start the task and results are generated in approx. 10mins. Pearson correlation will be printed in the terminal.
- The second functionality involving the TF-IDF had issues with OutOfMemory errors and as such will be commented out. Still working on it despite the project having ended. Intention is to output all the results into a file where said file will be transferred to your local repo for viewing.

