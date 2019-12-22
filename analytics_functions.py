import os
from fabric import Connection


# Create your key pair with EC2
def create_key_pair(name, ec2):
    response = ec2.create_key_pair(
        KeyName=name
    )
    key = response['KeyMaterial']

    # write the pem file
    fil = open(f'{name}.pem', "w")
    fil.write(key)
    fil.close()

    # must give permission
    os.system(f'chmod 400 {name}.pem')
    print(f'Key Pair {name} Created')
    return response


# Create security group in EC2 for your instance
def create_security_group(name, description, ip_permissions, ec2):
    response = ec2.describe_vpcs()
    vpc_id = response.get('Vpcs', [{}])[0].get('VpcId', '')

    response = ec2.create_security_group(GroupName=name, Description=description, VpcId=vpc_id)
    security_group_id = response['GroupId']
    print(f'Security Group Created {security_group_id} in vpc {vpc_id}.')

    data = ec2.authorize_security_group_ingress(
        GroupId=security_group_id,
        IpPermissions=ip_permissions)
    print(f'Ingress Successfully Set {data}')
    return security_group_id

# Create your EC2 instances
def create_instances(ami, max_count, instance_type, key, security_group_id, ec2):
    instances = ec2.run_instances(
        ImageId = ami,
        MinCount = 1,
        MaxCount = max_count,
        InstanceType = instance_type,
        KeyName = key,
        SecurityGroupIds = [security_group_id]
    )
    instance_list = []
    for i in instances['Instances']:
        instance_list.append(i['InstanceId'])
    
    print(f'Instances Created {instance_list}')
    return instance_list

# Returns a dictionary of your EC2 instance IPs
def list_ec2_instances(ec2):
    instances = {}
    res = ec2.describe_instances()
    for r in res['Reservations']:
        for ins in r['Instances']:
            if ins['State']['Name'] == 'running' or ins['State']['Name'] == 'pending':
                instances[ins['InstanceId']] = ins['PublicIpAddress']
    print(f'List of active Instances {instances}')
    return instances


# Returns a dictionary of your EC2 instance DNS
def get_publicdns(ec2):
    dnslist = {}
    res = ec2.describe_instances()
    for r in res['Reservations']:
        for ins in r['Instances']:
            if ins['State']['Name'] == 'running' or ins['State']['Name'] == 'pending':
                dnslist[ins['InstanceId']] = ins['PublicDnsName']
    print('List of active Instances %s' %dnslist)
    return dnslist



### Helpful functions for navigating instances with fabric
# Connect to an instance(this is used A LOT)

def theconnector(ip, key):
    c = Connection(
        host= ip,
        user="ubuntu",
        connect_kwargs={
            "key_filename": key + ".pem",
        },
    )
    return c


# For renaming a part of a file
def replace_single_term(template_file, output_file_name, term_to_replace, new_term):
    # open up our template mapred site
    filetomod = open(template_file)

    # append the lines to a file
    lines = [line for line in filetomod]

    # create new array
    newarray = []

    # search for the term, line by line, that you want to change eg <nnode>
    for line in lines:
        output = line
        if term_to_replace in line:
            output = line.replace(term_to_replace, new_term) # change to our namenode's dns
        # append to the new array
        newarray.append(output)

    # now to write to our main core-site.xml file
    xml = open(output_file_name, 'w')

    for newlines in newarray:
        xml.write(f'{newlines}')

    xml.close()
    filetomod.close()
    
    return
