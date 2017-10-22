import time

from botocore.exceptions import ClientError

def throttling_retry(func):
    """Retry when AWS is throttling API calls"""
    def retry_call(*args, **kwargs):
        retries = 0
        while True:
            try:
                retval = func(*args)
                return retval
            except ClientError as err:
                if (err.response['Error']['Code'] == 'Throttling' or err.response['Error']['Code'] == 'RequestLimitExceeded') and retries <= 3:
                    sleep = 3 * (2**retries)
                    print('Being throttled. Retrying after {} seconds..'.format(sleep))
                    time.sleep(sleep)
                    retries += 1
                else:
                    raise err
    return retry_call


@throttling_retry
def get_ami_id(client, name):
    """Return the first AMI ID given its name"""
    images = client.describe_images(Filters=[{'Name':'name', 'Values':[ name ]}])['Images']
    if len(images) != 0:
        return images[0]['ImageId']
    else:
        raise RuntimeError('{} AMI not found'.format(name))


@throttling_retry
def get_zone_id(client, name):
    """Return the first Route53 zone ID given its name"""
    prefix = '/hostedzone/'
    zones = client.list_hosted_zones_by_name(DNSName=name, MaxItems='1')['HostedZones']
    for zone in zones:
        if zone['Name'] == name:
            zone_id = zone['Id']
            if zone_id.startswith(prefix):
                return zone_id[len(prefix):]
            return zone_id
    raise RuntimeError('{} zone not found'.format(name))


@throttling_retry
def get_vpc_id(client, name):
    """Return the first VPC ID given its name and region"""
    vpcs = client.describe_vpcs(Filters=[{'Name':'tag:Name', 'Values':[ name ]}])['Vpcs']
    if len(vpcs) == 1:
        return vpcs[0]['VpcId']
    else:
        raise RuntimeError('{} VPC not found'.format(name))


@throttling_retry
def get_stack_output(client, name, key):
    """Return stack output key value"""
    stacks = client.describe_stacks(StackName=name)['Stacks']
    if len(stacks) != 1:
        raise RuntimeError('{} stack not found'.format(name))
    outputs = [s['Outputs'] for s in stacks][0]
    for output in outputs:
        if output['OutputKey'] == key:
            return output['OutputValue']
    raise RuntimeError('{} output not found'.format(key))


@throttling_retry
def get_stack_tag(client, name, key):
    """Return stack tag"""
    stacks = client.describe_stacks(StackName=name)['Stacks']
    if len(stacks) != 1:
        raise RuntimeError('{} stack not found'.format(name))
    tags = [s['Tags'] for s in stacks][0]
    for tag in tags:
        if tag['Key'] == key:
            return tag['Value']
    return ''


@throttling_retry
def get_stack_resource(client, stack_name, logical_id):
    """Return a physical_resource_id given its logical_id"""
    resource = client.describe_stack_resource(StackName=stack_name, LogicalResourceId=logical_id)['StackResourceDetail']
    if resource != None:
        return resource['PhysicalResourceId']
    return None

