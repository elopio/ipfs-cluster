#! /bin/sh

test_description="Test service migration v1 -> v2 and v2 -> v2"

. lib/test-lib.sh

test_ipfs_init
cleanup test_clean_ipfs
test_cluster_init
test_create_v1State
cleanup test_clean_cluster

test_expect_success IPFS,CLUSTER "cluster-service state preserved by migration" '
    cid=`docker exec ipfs sh -c "echo test | ipfs add -q"`
    ipfs-cluster-ctl pin add "$cid" &> test4 && sleep 2 &&
    kill -1 CLUSTER_D_PID && sleep 2 &&
    backup-file=$( ls test-config/backups/ | head -n 1)
    ipfs-cluster-service migration "test-config/backups/"$(backup-file) "test-config/ipfs-cluster-data"
    ipfs-cluster-service --config "test-config" >"$IPFS_OUTPUT" 2>&1 &
    ipfs-cluster-ctl pin ls "$cid" | grep -q "$cid" &&
    ipfs-cluster-ctl status "$cid" | grep -q -i "PINNED"
'

test_expect_success IPFS,CLUSTER,V1STATE "cluster-service loads v1 state correctly" '
    cid=`docker exec ipfs sh -c "echo test | ipfs add -q"` 
    kill -1 CLUSTER_D_PID &&
    ipfs-cluster-service migration "test-config/v1State" "test-config/ipfs-cluster-data"
    ipfs-cluster-service --config "test-config" >"$IPFS_OUTPUT" 2>&1 &
    ipfs-cluster-ctl pin ls "$cid" | grep -q "$cid" &&
    ipfs-cluster-ctl status "$cid" | grep -q -i "PINNED"
'

test_done
