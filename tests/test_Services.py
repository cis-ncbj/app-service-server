# Test suite for Services module

from Config import conf
import Globals as G
from Services import Service, ValidatorError
from Jobs import Job
from nose.tools import eq_, ok_, raises, assert_raises
import os


def setup_module():
    """
    Some configuration options to adapt environment for testing
    :return:
    """
    test_assets = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'assets')
    # change directory from which job data are loaded
    conf.gate_path_jobs = os.path.join(test_assets, 'payloads')
    conf.service_path_conf = os.path.join(test_assets, 'services')
    conf.service_path_data = os.path.join(test_assets, 'services','Data')
    conf.test_path_data = os.path.join(test_assets, 'data')
    G.init()


class TestValidator:
    @classmethod
    def setup_class(cls):
        cls.service = G.SERVICE_STORE['test']

    def test_validate(self):
        """
        Validator.validate for proper input
        :return:
        """
        job = Job('test_valid_job.json')
        G.VALIDATOR.validate(job)

        result = job.data.data
        valid_result = {
            "test_date": "20150317 135200",
            "test_object": {
                "A": 34,
                "B": [21, 30, 41],
                "C": "20151115 112000"
            },
            "test_float": 2.3,
            "test_float_array": [2.1, 44.5, 1.1],
            "test_object_array": [{"K": 3.3, "L": "20011119 103010"},
                                  {"K": 1.2, "L": "20110109 003010"},
                                  {"K": 88.11, "L": "20160522 063510"}]
        }
        for _k, _v in valid_result.items():
            eq_(_v, result[_k], u'Checking key {0:s}'.format(_k))

            # invalid input

    @raises(ValidatorError)
    def test_validate_invalid_object(self):
        """
        Validator.validate for invalid 'object' variable
        :return:
        """
        G.VALIDATOR.validate(Job('test_invalid_object.json'))

    @raises(ValidatorError)
    def test_validate_invalid_object_array(self):
        """
        Validator.validate for invalid 'object_array' variable
        :return:
        """
        G.VALIDATOR.validate(Job('test_invalid_object_array.json'))

    # TODO more tests with different kinds of invalid payloads

    def test_validate_value_success(self):
        """
        Generate check_validate_value_success tests based on json data
        :return: generator of check_validate_value_success tests for nose
        """
        name = os.path.join(conf.test_path_data, "validate_value_success.json")
        with open(name) as data_file:
            data = conf.json_load(data_file)

        for item in data:
            item.append("%s: %s != %s" % tuple(item))
            item.insert(0, self.check_validate_value_success)
            yield tuple(item)

    def test_validate_value_failure(self):
        """
        Generate check_validate_value_failure tests based on json data
        :return: generator of check_validate_value_failure tests for nose
        """
        name = os.path.join(conf.test_path_data, "validate_value_failure.json")
        with open(name) as data_file:
            data = conf.json_load(data_file)

        for item in data:
            item.insert(0, self.check_validate_value_failure)
            yield tuple(item)

    def check_validate_value_success(self, name, value, result, desc):
        """
        Test Validator.validate_value with correct payload
        :param name: name of variable to test
        :param value: payload
        :param result: expected result
        :param desc: description
        """
        template = self.service.variables[name]
        # proper values
        eq_(G.VALIDATOR.validate_value([name], value, template), result, desc)

    def check_validate_value_failure(self, name, value):
        """
        Test Validator.validate_value with incorrect payload
        :param name: name of variable to test
        :param value: payload
        :param desc: description
        """
        template = self.service.variables[name]
        # bad values
        assert_raises(ValidatorError, G.VALIDATOR.validate_value, [name], value, template)

    # Special cases
    def test_validate_value_float_tuple(self):
        """
        Validator.validate_value:  float_array variable
        :return:
        """
        var_name = 'test_float_array'
        template = self.service.variables[var_name]
        # proper value in a tuple
        input = (0.1, 55.3, 2.3)
        output = [0.1, 55.3, 2.3]
        eq_(G.VALIDATOR.validate_value([var_name], input, template),
            output, 'test_float_array: %s != %s' % (input, output))

    def test_validate_value_nested_object(self):
        """
        G.VALIDATOR.validate_value:  object variable
        :return:
        """
        # Too nested object
        template = dict(
            type="object",
            default={},
            values=dict(
                T=dict(
                    type="int",
                    default=4,
                    values=[0, 12]
                ),
                nested=dict(
                    type="object",
                    default={},
                    values=dict(
                        Z=dict(
                            type="int",
                            default=3,
                            values=[0, 12]
                        )
                    )
                )
            )
        )
        assert_raises(ValidatorError, G.VALIDATOR.validate_value, ['too_nested'],
                      dict(
                          T=1,
                          nested=dict(
                              Z=5
                          )),
                      template)

    def test_validate_value_unknown_type(self):
        """
        validate_value:  service providing unsupported variable type
        :return:
        """
        # Let us leave it as external case not parsed by Service so that when Service will validate input this test
        # still can be performed
        var_name = ['test_mistaken']  # service dev has misspelled 'float' and leaves 'flaot' in service file
        template = {
            'type': "flaot",
            'default': 2.2,
            'values': [0.0, 2.2]
        }
        # failing up to commit #106aa0669
        # Service developer messed up variable type and 'hacker' noticed
        assert_raises(ValidatorError, G.VALIDATOR.validate_value, var_name, "Hack payload", template)

