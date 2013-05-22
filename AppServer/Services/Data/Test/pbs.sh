#!/bin/sh
#PBS -q @@{queue}
#@@{PBS_OPTS}

echo "Commencing test: @@{TestName}" | tee -a progress.log
sleep @@{SLEEP}
echo 10 | tee -a progress.log
sleep @@{SLEEP}
echo 9 | tee -a progress.log
sleep @@{SLEEP}
echo 8 | tee -a progress.log
sleep @@{SLEEP}
echo 7 | tee -a progress.log
sleep @@{SLEEP}
echo 6 | tee -a progress.log
sleep @@{SLEEP}
echo 5 | tee -a progress.log
sleep @@{SLEEP}
echo 4 | tee -a progress.log
sleep @@{SLEEP}
echo 3 | tee -a progress.log
sleep @@{SLEEP}
echo 2 | tee -a progress.log
sleep @@{SLEEP}
echo 1 | tee -a progress.log

python -c "print @@{1Float} / @@{2Float}"
@@{COMMAND}

exit $?

