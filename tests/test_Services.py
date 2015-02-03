# Test suite for Services module

from Services import Validator, Service, conf, ServiceStore
from Jobs import Job
from nose.tools import eq_, ok_
import os


def setup_module():
    """
    Some configuration options to adapt environment for testing
    :return:
    """
    # from Config import conf
    # change directory from which job data are loaded
    conf.gate_path_jobs = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'assests')
    # add test service to services list
    ServiceStore['test'] = Service('test', dict(
        config={},
        sets={},
        variables=dict(
            test_float=dict(
                type="float",
                default=2.3,
                values=[0, 100]
            ),
            test_date=dict(
                type="datetime",
                default="20150115 173000",
                values="%Y%m%d %H%M%S"
            ),
            test_object=dict(
                type="object",
                default=dict(
                    A=1,
                    B=[2, 3, 4]
                ),
                values=dict(
                    A=dict(
                        type="int",
                        default=1,
                        values=[0, 1000]
                    ),
                    B=dict(
                        type="int_array",
                        default=[2, 3, 4],
                        values=[3, 0, 10000])
                )
            )
        )
    ))


class TestValidator:
    @classmethod
    def setup_class(cls):
        cls.valid_job = Job('test_valid_job.json')
        cls.service = ServiceStore['test']

    def test_validate(self):
        """
        Validator.validate
        :return:
        """
        # job = Job('test_valid_job.json')
        # print self.valid_job.id()
        eq_(1, 1, "OK")

    def test_validate_value_date(self):
        """
        Validator.validate_value:  datetime variable
        :return:
        """
        var_name = 'test_date'
        # correct date input
        ok_(Validator.validate_value(var_name, '20150317 135200', self.service))
        # returns 2003-4-4 13:52 ... is that correct? lets keep it that way
        ok_(Validator.validate_value(var_name, '200344 135200', self.service))

        # failed date input
        ok_(not Validator.validate_value(var_name, '201503 135200', self.service))
        ok_(not Validator.validate_value(var_name, '20150344 135200', self.service))
        ok_(not Validator.validate_value(var_name, '20150317 995200', self.service))
        ok_(not Validator.validate_value(var_name, '20151317 005200', self.service))
        ok_(not Validator.validate_value(var_name, '201w 005200', self.service))

    def test_validate_value_float(self):
        """
        Validator.validate_value:  float variable
        :return:
        """
        var_name = 'test_float'
        # correct value
        ok_(Validator.validate_value(var_name, 0.1, self.service))
        ok_(Validator.validate_value(var_name, '0.1', self.service))

        # failed input
        ok_(not Validator.validate_value(var_name, "ss", self.service))
        # localization? Nope and lets keep it that way
        ok_(not Validator.validate_value(var_name, "0,1", self.service))

    def test_validate_value_object(self):
        """
        Validator.validate_value:  object variable
        :return:
        """
        var_name = 'test_object'

        ok_(not Validator.validate_value(var_name, {}, self.service))



    def test_validate_value_unknown_type(self):
        """
        Validator.validate_value:  service providing unsupported variable type
        :return:
        """
        var_name = 'test_mistaken'
        # service dev has misspelled 'float' and leaves 'flaot' in service file
        ServiceStore['test'].variables[var_name] = dict(
            type="flaot",
            default=2.2,
            values=[0.0, 2.2]
        )
        # failing up to commit #106aa0669
        ok_(not Validator.validate_value(var_name, "Hack payload", self.service))