"""
Cloudformation related functions
"""
# An attempt to support python 2.7.x
from __future__ import print_function

import sys
import builtins
import time
import yaml
import json
import jinja2
import hashlib
import boto3

from os import path
from jinja2 import meta
from fnmatch import fnmatch
from tabulate import tabulate
from botocore.exceptions import ClientError
from operator import itemgetter
from datetime import datetime

from awscli.customizations.cloudformation.yamlhelper import intrinsics_multi_constructor

from stacks.aws import get_stack_tag
from stacks.aws import throttling_retry
from stacks.states import FAILED_STACK_STATES, COMPLETE_STACK_STATES, ROLLBACK_STACK_STATES, IN_PROGRESS_STACK_STATES

YES = ['y', 'Y', 'yes', 'YES', 'Yes']


def dict_to_list(dictionary):
    result = []
    for item in dictionary:
        result.append({'Key':item, 'Value':dictionary[item]})
    return result

def gen_template(tpl_file, config):
    """Return a tuple of json string template and options dict"""
    tpl_path, tpl_fname = path.split(tpl_file.name)
    env = _new_jinja_env(tpl_path)

    _check_missing_vars(env, tpl_file, config)

    tpl = env.get_template(tpl_fname)
    rendered = tpl.render(config)
    try:
        yaml.SafeLoader.add_multi_constructor("!", intrinsics_multi_constructor)
        docs = list(yaml.safe_load_all(rendered))
    except yaml.parser.ParserError as err:
        print(err)
        sys.exit(1)

    if len(docs) == 2:
        return json.dumps(docs[1], indent=2, sort_keys=True), docs[0]
    else:
        return json.dumps(docs[0], indent=2, sort_keys=True), None


def _check_missing_vars(env, tpl_file, config):
    """Check for missing variables in a template string"""
    tpl_str = tpl_file.read()
    ast = env.parse(tpl_str)
    required_properties = meta.find_undeclared_variables(ast)
    missing_properties = required_properties - config.keys() - set(dir(builtins))

    if len(missing_properties) > 0:
        print('Required properties not set: {}'.format(','.join(missing_properties)))
        sys.exit(1)


def _new_jinja_env(tpl_path):
    loader = jinja2.loaders.FileSystemLoader(tpl_path)
    env = jinja2.Environment(loader=loader)
    return env


# TODO(vaijab): fix 'S3ResponseError: 301 Moved Permanently', this happens when
# a connection to S3 is being made from a different region than the one a bucket
# was created in.
def upload_template(config, tpl, stack_name):
    """Upload a template to S3 bucket and returns S3 key url"""
    bn = config.get('templates_bucket_name', '{}-stacks-{}'.format(config['env'], config['region']))

    s3 = config['s3_resource']

    h = _calc_md5(tpl)
    key = '{}/{}/{}'.format(config['env'], stack_name, h)

    s3.Object(bn, key).put(Body=tpl)

    url = '{}/{}/{}'.format(s3.meta.endpoint_url, bn, key)

    return url


def stack_resources(client, stack_name, logical_resource_id=None):
    """List stack resources"""
    try:
        if logical_resource_id is None:
            result = client.describe_stack_resources(StackName=stack_name)
        else:
            result = client.describe_stack_resources(StackName=stack_name, LogicalResourceId=logical_resource_id)
    except ClientError as err:
        print(err.response['Error']['Message'])
        sys.exit(1)
    resources = []
    if logical_resource_id:
        resources.append([r['PhysicalResourceId'] for r in result['StackResources']])
    else:
        for r in result['StackResources']:
            columns = [
                r['LogicalResourceId'],
                r['PhysicalResourceId'],
                r['ResourceType'],
                r['ResourceStatus'],
            ]
            resources.append(columns)

    if len(result) >= 1:
        return tabulate(resources, tablefmt='plain')
    return None


def stack_outputs(client, stack_name, output_name):
    """List stacks outputs"""
    try:
        result = client.describe_stacks(StackName=stack_name)
    except ClientError as err:
        print(err.response['Error']['Message'])
        sys.exit(1)

    outputs = []
    outs = [s['Outputs'] for s in result['Stacks']][0]
    for o in outs:
        if not output_name:
            columns = [o['OutputKey'], o['OutputValue']]
            outputs.append(columns)
        elif output_name and o['OutputKey'] == output_name:
            outputs.append([o['OutputValue']])

    if len(result) >= 1:
        return tabulate(outputs, tablefmt='plain')
    return None


def list_stacks(client, name_filter='*', verbose=False):
    """List active stacks"""
    states = FAILED_STACK_STATES + COMPLETE_STACK_STATES + IN_PROGRESS_STACK_STATES + ROLLBACK_STACK_STATES
    s = client.list_stacks(StackStatusFilter=states)['StackSummaries']

    stacks = []
    for n in s:
        if name_filter and fnmatch(n['StackName'], name_filter):
            columns = [n['StackName'], n['StackStatus']]
            if verbose:
                env = get_stack_tag(client, n['StackName'], 'Env')
                columns.append(env)
                columns.append(n.template_description)
            stacks.append(columns)

    if len(stacks) >= 1:
        return tabulate(stacks, tablefmt='plain')
    return None


def create_stack(client, stack_name, tpl_file, config, update=False, dry=False, create_on_update=False):
    """Create or update CloudFormation stack from a jinja2 template"""
    tpl, metadata = gen_template(tpl_file, config)

    # Set default tags which cannot be overwritten
    default_tags = {
        'Env': config['env'],
        'MD5Sum': _calc_md5(tpl)
    }

    if metadata:
        tags = _extract_tags(metadata)
        tags.update(default_tags)
        name_from_metadata = metadata.get('name')
        disable_rollback = metadata.get('disable_rollback')
    else:
        name_from_metadata = None
        tags = default_tags
        disable_rollback = False

    if not stack_name:
        stack_name = name_from_metadata
    if not stack_name:
        print('Stack name must be specified via command line argument or stack metadata.')
        sys.exit(1)

    tpl_size = len(tpl)

    if dry:
        print(tpl, flush=True)
        print('Name: {}'.format(stack_name), file=sys.stderr, flush=True)
        print('Tags: ' + ', '.join(['{}={}'.format(k, v) for (k, v) in tags.items()]), file=sys.stderr, flush=True)
        print('Template size:', tpl_size, file=sys.stderr, flush=True)
        return True

    tags = dict_to_list(tags)

    try:
        if tpl_size > 51200:
            tpl_url = upload_template(config, tpl, stack_name)
            if update and create_on_update and not stack_exists(client, stack_name):
                client.create_stack(StackName=stack_name, TemplateURL=tpl_url,
                                  Tags=tags, Capabilities=['CAPABILITY_IAM'],
                                  DisableRollback=disable_rollback)
            elif update:
                client.update_stack(StackName=stack_name, TemplateURL=tpl_url,
                                  Tags=tags, Capabilities=['CAPABILITY_IAM'],
                                  DisableRollback=disable_rollback)
            else:
                client.create_stack(StackName=stack_name, TemplateURL=tpl_url,
                                  Tags=tags, Capabilities=['CAPABILITY_IAM'],
                                  DisableRollback=disable_rollback)
        else:
            if update and create_on_update and not stack_exists(client, stack_name):
                client.create_stack(StackName=stack_name, TemplateBody=tpl,
                                  Tags=tags, Capabilities=['CAPABILITY_IAM'],
                                  DisableRollback=disable_rollback)
            elif update:
                client.update_stack(StackName=stack_name, TemplateBody=tpl,
                                  Tags=tags, Capabilities=['CAPABILITY_IAM'])
            else:
                client.create_stack(StackName=stack_name, TemplateBody=tpl,
                                  Tags=tags, Capabilities=['CAPABILITY_IAM'],
                                  DisableRollback=disable_rollback)
    except ClientError as err:
        # Do not exit with 1 when one of the below messages are returned
        non_error_messages = [
            'No updates are to be performed',
            'already exists',
        ]
        if any(s in err.response['Error']['Message'] for s in non_error_messages):
            print(err.response['Error']['Message'])
            sys.exit(0)
        print(err.response['Error']['Message'])
        sys.exit(1)
    return stack_name


def _extract_tags(metadata):
    """Return tags from a metadata"""
    tags = {}

    for tag in metadata.get('tags', []):
        tags[tag['key']] = tag['value']
    return tags


def _calc_md5(j):
    """Calculate an MD5 hash of a string"""
    return hashlib.md5(j.encode()).hexdigest()


def delete_stack(client, stack_name, region, profile, confirm):
    """Deletes stack given its name"""
    msg = ('You are about to delete the following stack:\n'
           'Name: {}\n'
           'Region: {}\n'
           'Profile: {}\n').format(stack_name, region, profile)
    if not confirm:
        print(msg)
        response = input('Are you sure? [y/N] ')
    else:
        response = 'yes'

    if response in YES:
        try:
            client.delete_stack(StackName=stack_name)
        except ClientError as err:
            if 'does not exist' in err.response['Error']['Message']:
                print(err.response['Error']['Message'])
                sys.exit(0)
            else:
                print(err.response['Error']['Message'])
                sys.exit(1)
    else:
        sys.exit(0)


def get_events(client, stack_name, next_token):
    """Get stack events"""
    try:
        if next_token is None:
            events = client.describe_stack_events(StackName=stack_name)
        else:    
            events = client.describe_stack_events(StackName=stack_name, NextToken=next_token)
        next_token = events.get('NextToken', None)
        return sorted_events(events['StackEvents']), next_token
    except ClientError as err:
        if 'does not exist' in err.response['Error']['Message']:
            print(err.response['Error']['Message'])
            sys.exit(0)
        else:
            print(err.response['Error']['Message'])
            sys.exit(1)


def sorted_events(events):
    """Sort stack events by timestamp"""
    return sorted(events, key=itemgetter('Timestamp'))


def print_events(client, stack_name, follow, lines=100, from_timestamp=0):
    """Prints tabulated list of events"""
    events_display = []
    seen_ids = set()
    next_token = None
    from_timestamp = datetime.fromtimestamp(from_timestamp)

    while True:
        events, next_token = get_events(client, stack_name, next_token)
        status = get_stack_status(client, stack_name)
        if follow:
            events_display = [(event.get('Timestamp', ''), event.get('ResourceStatus', ''), event.get('ResourceType', ''),
                               event.get('LogicalResourceId', ''), event.get('ResourceStatusReason', '')) for event in events
                              if event['EventId'] not in seen_ids and event['Timestamp'] >= from_timestamp]
            if len(events_display) > 0:
                print(tabulate(events_display, tablefmt='plain'), flush=True)
                seen_ids |= set([event['EventId'] for event in events])
            if status not in IN_PROGRESS_STACK_STATES and next_token is None:
                break
            if next_token is None:
                time.sleep(5)
        else:
            events_display.extend([(event.get('Timestamp', ''), event.get('ResourceStatus', ''), event.get('ResourceType', ''),
                                    event.get('LogicalResourceId', ''), event.get('ResourceStatusReason', ''))
                                   for event in events])
            if len(events_display) >= lines or next_token is None:
                break

    if not follow:
        print(tabulate(events_display[:lines], tablefmt='plain'), flush=True)

    return status


@throttling_retry
def get_stack_status(client, stack_name):
    """Check stack status"""
    stacks = []
    resp = client.describe_stacks()
    stacks.extend(resp['Stacks'])
    while 'NextToken' in resp.keys():
        resp = client.describe_stacks(NextToken=resp['NextToken'])
        stacks.extend(resp['Stacks'])
    for s in stacks:
        if s['StackName'] == stack_name and s['StackStatus'] != 'DELETE_COMPLETE':
            return s['StackStatus']
    return None


def stack_exists(client, stack_name):
    """Check whether stack_name exists

    CF keeps deleted duplicate stack names with DELETE_COMPLETE status, which is
    treated as non existing stack.
    """
    status = get_stack_status(client, stack_name)
    if status == 'DELETE_COMPLETE' or status is None:
        return False
    return True
