#/bin/bash
set -o pipefail
set -x
set -e
while go test -v -run TestSnapshotRecoverManyClients3B | tee 3B-fail23.log
do
    sleep 1
done
