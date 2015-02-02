#Test suite for Services module
from Services import Validator, Service, conf

from nose.tools import eq_



class TestValidator:

    @classmethod
    def setup_class(cls):
        cls.service = Service('test',dict(
            config = {},
            sets = {},
            variables = dict(
                test_float = dict(
                    type = "float",
                    default = 2.3,
                    variables = [0,100]
                ),
                test_date = dict(
                    type = "datetime",
                    default = "20150115 173000",
                    variables = "%Y%m%d %H%M%S"
                ),
                test_object= dict(
                     type = "object",
                     default =dict(
                         A=1,
                         B=[2, 3, 4]
                     ),
                     values =dict(
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

    # @classmethod
    # def teardown_class(cls):
    #     print ("teardown_class() after any methods in this class")

    def test_validate(self):
        print conf.gate_path
        eq_(1,1,"OK")

