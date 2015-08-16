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
    # from Config import conf
    # change directory from which job data are loaded
    conf.gate_path_jobs = os.path.join(test_assets, 'payloads')
    conf.service_path_conf = os.path.join(test_assets, 'services')
    conf.service_path_data = os.path.join(test_assets, 'services','Data')
    G.init()
    # add test services to services list
    #for _service in ['test', 'default']:
    #    with open(os.path.join(test_assets, 'services', _service + '.json')) as _f:
    #        ServiceStore[_service] = Service(_service, conf.json_load(_f))


class TestValidator:
    @classmethod
    def setup_class(cls):
        cls.valid_job = Job('test_valid_job.json')
        cls.service = G.SERVICE_STORE['test']

    def test_validate(self):
        """
        Validator.validate for proper input
        :return:
        """
        # job = Job('test_valid_job.json')
        # print self.valid_job.id()
        G.VALIDATOR.validate(self.valid_job)

        result = self.valid_job.data.data
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

    def test_validate_value_date(self):
        """
        Validator.validate_value:  datetime variable
        :return:
        """
        var_name = ['test_date']
        template = self.service.variables[var_name[0]]
        # correct date input
        eq_(G.VALIDATOR.validate_value(var_name, '20150317 135200', template),
            '20150317 135200', 'Proper date')
        # returns 2003-4-4 13:52 ... is that correct? lets keep it that way
        eq_(G.VALIDATOR.validate_value(var_name, '200344 135200', template),
            '200344 135200', 'Month and day without leading zeros')

        # failed date input
        # Too short date
        assert_raises(ValidatorError, G.VALIDATOR.validate_value, var_name, '201503 135200', template)
        # Day out of bounds
        assert_raises(ValidatorError, G.VALIDATOR.validate_value, var_name, '20150344 135200', template)
        # Hour our of bounds
        assert_raises(ValidatorError, G.VALIDATOR.validate_value, var_name, '20150317 995200', template)
        # Month out of bound
        assert_raises(ValidatorError, G.VALIDATOR.validate_value, var_name, '20151317 005200', template)
        # Random string
        assert_raises(ValidatorError, G.VALIDATOR.validate_value, var_name, '201w 005200', template)

    def test_validate_value_float_array(self):
        """
        Validator.validate_value:  float_array variable
        :return:
        """
        var_name = ['test_float_array']
        template = self.service.variables[var_name[0]]
        # proper value
        eq_(G.VALIDATOR.validate_value(var_name, [0.1, 55.3, 2.3], template),
            [0.1, 55.3, 2.3], "Basic value - array")
        eq_(G.VALIDATOR.validate_value(var_name, (0.1, 55.3, 2.3), template),
            [0.1, 55.3, 2.3], "Basic value - tuple")

        # failed input
        # Number instead of array: failed up to commit #106aa0669
        assert_raises(ValidatorError, G.VALIDATOR.validate_value, var_name, 0.1, template)
        # Too many values
        assert_raises(ValidatorError, G.VALIDATOR.validate_value, var_name, (0.1, 55.3, 2.3, 33.21, 5.1, 7.5),
                      template)
        # One value out of bounds
        assert_raises(ValidatorError, G.VALIDATOR.validate_value, var_name, (599999999.1, 7.5), template)
        # One value out of bounds
        assert_raises(ValidatorError, G.VALIDATOR.validate_value, var_name, (5.1, -7.5), template)

    def test_validate_value_float(self):
        """
        G.VALIDATOR.validate_value:  float variable
        :return:
        """
        var_name = ['test_float']
        template = self.service.variables[var_name[0]]
        # correct value
        eq_(G.VALIDATOR.validate_value(var_name, 0.1, template),
            0.1, "Proper value")
        eq_(G.VALIDATOR.validate_value(var_name, '0.1', template),
            0.1, 'Float as a string')

        # failed input
        # Random string
        assert_raises(ValidatorError, G.VALIDATOR.validate_value, var_name, "ss", template)
        # localization? Nope and lets keep it that way
        assert_raises(ValidatorError, G.VALIDATOR.validate_value, var_name, "0,1", template)

    def test_validate_value_object(self):
        """
        G.VALIDATOR.validate_value:  object variable
        :return:
        """
        var_name = ['test_object']
        template = self.service.variables[var_name[0]]
        # proper values
        eq_(G.VALIDATOR.validate_value(
            var_name, dict(A=2, B=[4, 5, 34]), template),
            dict(A=2, B=[4, 5, 34], C="20151115 112000"), "Basic object")
        eq_(G.VALIDATOR.validate_value(
            var_name, {}, template),
            dict(A=1, B=[2, 3, 4], C="20151115 112000"), "Empty object")

        # failed input
        # Unsupported variable
        assert_raises(ValidatorError, G.VALIDATOR.validate_value, var_name, dict(C='bla'), template)
        # Array instead of dict
        assert_raises(ValidatorError, G.VALIDATOR.validate_value, var_name, [], template)
        # Not valid variable
        assert_raises(ValidatorError, G.VALIDATOR.validate_value, var_name, dict(A=99999999), template)
        # Not valid variable
        assert_raises(ValidatorError, G.VALIDATOR.validate_value, var_name, dict(B=[1, 2, 3, 4, 5, 4]), template)

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

    def test_validate_value_object_array(self):
        """
        G.VALIDATOR.validate_value:  object_array variable
        :return:
        """
        var_name = ['test_object_array']
        template = self.service.variables[var_name[0]]
        # proper values
        eq_(G.VALIDATOR.validate_value(var_name,
                                          [dict(K=2, L="21011119 133010"),
                                           dict(K=0.2, L="20050119 033010"),
                                           dict(K=20.1, L="22011116 103110")],
                                          template),
            [
                dict(K=2, L="21011119 133010"),
                dict(K=0.2, L="20050119 033010"),
                dict(K=20.1, L="22011116 103110")
            ],
            "Basic object")
        eq_(G.VALIDATOR.validate_value(var_name, [{}, {}], template),
            [dict(K=1.2, L="20011119 103010"), dict(K=1.2, L="20011119 103010")],
            "Empty objects")
        eq_(G.VALIDATOR.validate_value(var_name, [{}, dict(K=2, L="21011119 133010")],
                                          template),
            [dict(K=1.2, L="20011119 103010"), dict(K=2, L="21011119 133010")],
            "Mixed empty and full objects")

        # failed input
        # Object instead of array
        assert_raises(ValidatorError, G.VALIDATOR.validate_value, var_name, {'ss': 1}, template)
        # One failed value in one object in array
        assert_raises(ValidatorError, G.VALIDATOR.validate_value, var_name,
                      [dict(K=2, L="21011119 133010"),
                       dict(K=0.2, L="20050119 433010"),  # hour out of bounds
                       dict(K=20.1, L="22011116 103110")],
                      template)

    def test_validate_value_unknown_type(self):
        """
        G.VALIDATOR.validate_value:  service providing unsupported variable type
        :return:
        """
        var_name = ['test_mistaken']  # service dev has misspelled 'float' and leaves 'flaot' in service file
        template = {
            'type': "flaot",
            'default': 2.2,
            'values': [0.0, 2.2]
        }
        # failing up to commit #106aa0669
        # Service developer messed up variable type and 'hacker' noticed
        assert_raises(ValidatorError, G.VALIDATOR.validate_value, var_name, "Hack payload", template)
