import unittest
import boto3
from moto import mock_cloudformation

from stacks import cf


def list_to_dict(list):
    result = {}
    for item in list:
        result[item['Key']] = item['Value']
    return result

class TestTemplate(unittest.TestCase):

    def test_gen_valid_template(self):
        config = {'env': 'dev', 'test_tag': 'testing'}
        tpl_file = open('tests/fixtures/valid_template.yaml')
        tpl, options = cf.gen_template(tpl_file, config)
        self.assertIsInstance(tpl, str)
        self.assertIsInstance(options, dict)

    def test_gen_invalid_template(self):
        config = {'env': 'dev', 'test_tag': 'testing'}
        tpl_file = open('tests/fixtures/invalid_template.yaml')

        with self.assertRaises(SystemExit) as err:
            tpl, options = cf.gen_template(tpl_file, config)
        self.assertEqual(err.exception.code, 1)

    def test_gen_template_missing_properties(self):
        config = {'env': 'unittest'}
        tpl_file = open('tests/fixtures/valid_template.yaml')

        with self.assertRaises(SystemExit) as err:
            tpl, options = cf.gen_template(tpl_file, config)
        self.assertEqual(err.exception.code, 1)


@mock_cloudformation
class TestStackActions(unittest.TestCase):

    def setUp(self):
        self.config = {
            'env': 'unittest',
            'custom_tag': 'custom-tag-value',
            'region': 'us-east-1',
        }
        session = boto3.Session(region_name=self.config['region'])
        for service_name in [ 'ec2', 'cloudformation', 'route53', 's3' ]:
            self.config[service_name + '_client'] = session.client(service_name)

    def test_create_stack(self):
        stack_name = None
        with open('tests/fixtures/create_stack_template.yaml') as tpl_file:
            cf.create_stack(self.config['cloudformation_client'], stack_name, tpl_file, self.config)

        stack = self.config['cloudformation_client'].describe_stacks(StackName='unittest-infra')['Stacks'][0]
        tags = list_to_dict(stack['Tags'])
        self.assertEqual('unittest-infra', stack['StackName'])
        self.assertEqual(self.config['env'], tags['Env'])
        self.assertEqual(self.config['custom_tag'], tags['Test'])
        self.assertEqual('b08c2e9d7003f62ba8ffe5c985c50a63', tags['MD5Sum'])

    def test_update_stack(self):
        stack_name = None
        with open('tests/fixtures/create_stack_template.yaml') as tpl_file:
            cf.create_stack(self.config['cloudformation_client'], stack_name, tpl_file,
                            self.config, update=True)
        stack = self.config['cloudformation_client'].describe_stacks(StackName='unittest-infra')['Stacks'][0]
        tags = list_to_dict(stack['Tags'])
        self.assertEqual('b08c2e9d7003f62ba8ffe5c985c50a63', tags['MD5Sum'])

    def test_create_on_update(self):
        stack_name = 'create-on-update-stack'
        with open('tests/fixtures/create_stack_template.yaml') as tpl_file:
            cf.create_stack(self.config['cloudformation_client'], stack_name, tpl_file,
                            self.config, update=True, create_on_update=True)
        stack = self.config['cloudformation_client'].describe_stacks(StackName=stack_name)['Stacks'][0]
        tags = list_to_dict(stack['Tags'])
        self.assertEqual('b08c2e9d7003f62ba8ffe5c985c50a63', tags['MD5Sum'])

    def test_create_stack_no_stack_name(self):
        stack_name = None
        with open('tests/fixtures/no_metadata_template.yaml') as tpl_file:
            with self.assertRaises(SystemExit) as err:
                cf.create_stack(self.config['cloudformation_client'], stack_name, tpl_file, self.config)
            self.assertEqual(err.exception.code, 1)

    def test_create_stack_no_metadata(self):
        stack_name = 'my-stack'
        with open('tests/fixtures/no_metadata_template.yaml') as tpl_file:
            cf.create_stack(self.config['cloudformation_client'], stack_name, tpl_file, self.config)
        stack = self.config['cloudformation_client'].describe_stacks(StackName='my-stack')['Stacks'][0]
        tags = list_to_dict(stack['Tags'])
        self.assertEqual('my-stack', stack['StackName'])
        self.assertEqual(self.config['env'], tags['Env'])
        self.assertEqual('b08c2e9d7003f62ba8ffe5c985c50a63', tags['MD5Sum'])

if __name__ == '__main__':
    unittest.main()
