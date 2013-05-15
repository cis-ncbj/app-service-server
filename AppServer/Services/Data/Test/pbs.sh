#!/bin/sh
#@@{PBS_OPTS}

echo "Commencing test: @@{TestName}" | tee progress.log
sleep @@{SLEEP}
echo 10 | tee progress.log
sleep @@{SLEEP}
echo 9 | tee progress.log
sleep @@{SLEEP}
echo 8 | tee progress.log
sleep @@{SLEEP}
echo 7 | tee progress.log
sleep @@{SLEEP}
echo 6 | tee progress.log
sleep @@{SLEEP}
echo 5 | tee progress.log
sleep @@{SLEEP}
echo 4 | tee progress.log
sleep @@{SLEEP}
echo 3 | tee progress.log
sleep @@{SLEEP}
echo 2 | tee progress.log
sleep @@{SLEEP}
echo 1 | tee progress.log

echo "@@{1Float} / @@{2Float}" | bc -l
@@{COMMAND}

exit $?

