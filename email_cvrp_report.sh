#!/bin/sh

TS=`date +%Y-%m-%d-%H_%M`
echo $TS

scripts_dir=`dirname $0`

if [ -z "$1" ] ; then
  echo "No report to send..."
  exit 1
fi

report=$1
from='CiBeez post <post@cibeez.com>'
to='pessy.hollander@gmail.com'
cc='pessy@cibeez.com'
subject="Remedix CVRP report $TS"


echo "${report}" \
  | mail -s "${subject}"  \
    -aFrom:"${from}" "${to}" -aCc:"${cc}"

