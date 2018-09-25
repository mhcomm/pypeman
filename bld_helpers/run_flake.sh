#!/bin/bash

# ############################################################################
# Helper script to run flake with an allowed amount of errors
#
# this script can be removed as soon as all error counters are 0
# ############################################################################

check_fail() {
    error=$1
    msg="$2"
    #echo "fail $error $msg"
    if [[ $error -ne 0 ]] ; then
        echo "$msg"
        exit $error
    fi
}

# allows to run tox without flake during development
# by setting env var RUN_NO_FLAKE8 to 1
if [[ $RUN_NO_FLAKE8 = 1 ]]  ; then
    echo will skip running flake
    exit 0
fi

python -m flake8 pypeman | tee flake.log
wcnt=$(cat flake.log | grep -Ee 'W[0-9]{3}' | wc -l) 
ecnt=$(cat flake.log | grep -Ee 'E[0-9]{3}' | wc -l) 
fcnt=$(cat flake.log | grep -Ee 'F[0-9]{3}' | wc -l)
f821cnt=$(cat flake.log | grep -Ee 'F821' | wc -l)
e999cnt=$(cat flake.log | grep -Ee 'E999' | wc -l)

if [ $wcnt -gt 0 ] ; then
    echo "===== first 50 errors of type W ====="
    cat flake.log | grep -Ee 'W[0-9]{3}' | head -n 50
    echo
fi
if [ $ecnt -gt 0 ] ; then
    echo "===== first 50 errors of type E ====="
    cat flake.log | grep -Ee 'E[0-9]{3}' | head -n 50
    echo
fi
if [ $fcnt -gt 0 ] ; then
    echo "===== first 50 errors of type F ====="
    cat flake.log | grep -Ee 'F[0-9]{3}' | head -n 50
    echo
fi
if [ $f821cnt -gt 0 ] ; then
    echo "===== all F821 errors ====="
    cat flake.log | grep -Ee 'F821'
    echo
fi
if [ $e999cnt -gt 0 ] ; then
    echo "===== all syntax errors ====="
    bld_helpers/run_flake.sh
    cat flake.log | grep -Ee 'E999'
    echo
fi

echo "level ...W $wcnt"
echo "level ...E $ecnt"
echo "level ...F $fcnt"
echo "level F821 $f821cnt"
echo "syntax err $e999cnt"

test $wcnt -le 35 || exit 1
test $ecnt -le 283 || exit 1
test $fcnt -le 80 || exit 1
test $f821cnt -le 0 || exit 1
test $e999cnt -le 0 || exit 1

