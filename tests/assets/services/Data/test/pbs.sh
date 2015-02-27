#!/bin/sh
@@{test_float}
@@{test_date}
@@{test_object.A}:
{% for b in test_object.B %}
    B: @@{b} ?
{% endfor %}