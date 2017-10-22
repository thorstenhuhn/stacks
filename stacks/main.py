# An attempt to support python 2.7.x
from __future__ import print_function

import sys
import os
import signal
from time import time

import boto3

from stacks import cli
from stacks import aws
from stacks import cf
from stacks.config import config_load
from stacks.config import get_region_name
from stacks.config import get_default_region_name
from stacks.config import profile_exists
from stacks.config import validate_properties
from stacks.config import print_config
from stacks.states import FAILED_STACK_STATES, ROLLBACK_STACK_STATES


def main():
    for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGQUIT]:
        signal.signal(sig, handler)

    parser, args = cli.parse_options()

    if not args.subcommand:
        parser.print_help()
        sys.exit(0)

    config_file = vars(args).get('config', None)
    config_dir = vars(args).get('config_dir', None)
    env = vars(args).get('env', None)
    config = config_load(env, config_file, config_dir)

    if args.subcommand == 'config':
        print_config(config, args.property_name, output_format=args.output_format)
        sys.exit(0)

    config['get_ami_id'] = aws.get_ami_id
    config['get_vpc_id'] = aws.get_vpc_id
    config['get_zone_id'] = aws.get_zone_id
    config['get_stack_output'] = aws.get_stack_output
    config['get_stack_tag'] = aws.get_stack_tag
    config['get_stack_resource'] = aws.get_stack_resource

    # Figure out profile value in the following order
    # - cli arg
    # - env variable
    # - default profile if exists
    if args.profile:
        profile = args.profile
    elif os.environ.get('AWS_DEFAULT_PROFILE'):
        profile = os.environ.get('AWS_DEFAULT_PROFILE')
    elif profile_exists('default'):
        profile = 'default'
    elif args.profile and not profile_exists(args.profile):
        print('Profile "{}" does not exist.'.format(args.profile))
        sys.exit(1)
    else:
        profile = None

    # Figure out region value in the following order
    # - cli arg
    # - env variable
    # - region from config
    if args.region:
        region = args.region
    elif os.environ.get('AWS_DEFAULT_REGION'):
        region = os.environ.get('AWS_DEFAULT_REGION')
    elif get_region_name(profile):
        region = get_region_name(profile)
    else:
        region = get_default_region_name()

    if not region:
        print('Region is not specified.')
        sys.exit(1)

    config['region'] = region

    # Not great, but try to catch everything.
    try:
        session = boto3.Session(region_name=region, profile_name=profile)
        for service_name in [ 'ec2', 'cloudformation', 'route53', 's3' ]:
            config[service_name + '_client'] = session.client(service_name)
    # TODO(alekna): Fix too broad exception
    except:
        print(sys.exc_info()[1])
        sys.exit(1)

    if args.subcommand == 'resources':
        output = cf.stack_resources(config['cloudformation_client'], args.name, args.logical_id)
        if output:
            print(output)

    if args.subcommand == 'outputs':
        output = cf.stack_outputs(config['cloudformation_client'], args.name, args.output_name)
        if output:
            print(output)

    if args.subcommand == 'list':
        output = cf.list_stacks(config['cloudformation_client'], args.name, args.verbose)
        if output:
            print(output)

    if args.subcommand == 'create' or args.subcommand == 'update':
        if args.property:
            properties = validate_properties(args.property)
            config.update(properties)

        if args.subcommand == 'create':
            stack_name = cf.create_stack(config['cloudformation_client'], args.name, args.template, config, dry=args.dry_run)
            if args.events_follow and not args.dry_run:
                stack_status = cf.print_events(config['cloudformation_client'], stack_name, args.events_follow)
                if stack_status in FAILED_STACK_STATES + ROLLBACK_STACK_STATES:
                    sys.exit(1)
        else:
            from_timestamp = time()
            stack_name = cf.create_stack(config['cloudformation_client'], args.name, args.template, config, update=True, dry=args.dry_run,
                                         create_on_update=args.create_on_update)
            if args.events_follow and not args.dry_run:
                stack_status = cf.print_events(config['cloudformation_client'], stack_name, args.events_follow, from_timestamp=from_timestamp)
                if stack_status in FAILED_STACK_STATES + ROLLBACK_STACK_STATES:
                    sys.exit(1)

    if args.subcommand == 'delete':
        from_timestamp = time()
        cf.delete_stack(config['cloudformation_client'], args.name, region, profile, args.yes)
        if args.events_follow:
            stack_status = cf.print_events(config['cloudformation_client'], args.name, args.events_follow, from_timestamp=from_timestamp)
            if stack_status in FAILED_STACK_STATES:
                sys.exit(1)

    if args.subcommand == 'events':
        cf.print_events(config['cloudformation_client'], args.name, args.events_follow, args.lines)


def handler(signum, _):
    print('Signal {} received. Stopping.'.format(signum))
    sys.exit(0)
