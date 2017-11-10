# Sharness test framework for ipfs-cluster
#
# We are using sharness (https://github.com/mlafeldt/sharness)
# which was extracted from the Git test framework.

SHARNESS_LIB="lib/sharness/sharness.sh"

# Daemons output will be redirected to...
IPFS_OUTPUT="/dev/null" # change for debugging
# IPFS_OUTPUT="/dev/stderr" # change for debugging

. "$SHARNESS_LIB" || {
    echo >&2 "Cannot source: $SHARNESS_LIB"
    echo >&2 "Please check Sharness installation."
    exit 1
}

which jq >/dev/null 2>&1
if [ $? -eq 0 ]; then
    test_set_prereq JQ
fi

# Set prereqs
test_ipfs_init() {
    which docker >/dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "Docker not found"
        exit 1
    fi
    if docker ps --format '{{.Names}}' | egrep -q '^ipfs$'; then
        echo "ipfs container already running"
    else
        docker run --name ipfs -d -p 127.0.0.1:5001:5001 ipfs/go-ipfs > /dev/null 2>&1
        if [ $? -ne 0 ]; then
            echo "Error running go-ipfs in docker."
            exit 1
        fi
        sleep 10
    fi
    test_set_prereq IPFS
}

test_ipfs_running() {
    if curl -s "localhost:5001/api/v0/version" > /dev/null; then
        test_set_prereq IPFS
    else
        echo "IPFS is not running"
        exit 1
    fi
}

test_cluster_init() {
    custom_config_files="$1"

    which ipfs-cluster-service >/dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "ipfs-cluster-service not found"
        exit 1
    fi
    which ipfs-cluster-ctl >/dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "ipfs-cluster-ctl not found"
        exit 1
    fi
    ipfs-cluster-service -f --config "test-config" init >"$IPFS_OUTPUT" 2>&1
    if [ $? -ne 0 ]; then
        echo "error initializing ipfs cluster"
        exit 1
    fi
    rm -rf "test-config/ipfs-cluster-data"
    if [ -n "$custom_config_files" ]; then
        cp -f ${custom_config_files}/* "test-config"
    fi
    ipfs-cluster-service --config "test-config" >"$IPFS_OUTPUT" 2>&1 &
    export CLUSTER_D_PID=$!
    sleep 5
    test_set_prereq CLUSTER
}

test_cluster_config() {
    export CLUSTER_CONFIG_PATH="test-config/service.json"
    export CLUSTER_CONFIG_ID=`jq --raw-output ".cluster.id" $CLUSTER_CONFIG_PATH`
    export CLUSTER_CONFIG_PK=`jq --raw-output ".cluster.private_key" $CLUSTER_CONFIG_PATH`
    [ "$CLUSTER_CONFIG_ID" != "null" ] && [ "$CLUSTER_CONFIG_PK" != "null" ]
}

cluster_id() {
    jq --raw-output ".cluster.id" test-config/service.json
}

# Note: should only be called in when CLUSTER prereq is already true because
# it depends on test-config existing to add the temporary v1State file.
test_create_v1State() {
    echo '{ "Version": 1, "PinMap": { "QmeomffUNfmQy76CQGy9NdmqEnnHU9soCexBnGU3ezPHVH": {} }}' > test-config/v1State
    test_set_prereq V1STATE
}

# Cleanup functions
test_clean_ipfs(){
    docker kill ipfs
    docker rm ipfs
    sleep 1
}

test_clean_cluster(){
    kill -1 "$CLUSTER_D_PID"
    rm -rf 'test-config'
    sleep 2
}
