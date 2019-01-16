#!/usr/bin/env bash

. /opt/toolchain/toolchain.sh
export PROTOC_HOME="/opt/toolchain/protobuf-2.5.0"
export PATH="${PROTOC_HOME}/bin:$PATH"

if [[ ! -d "$PROTOC_HOME" ]]; then
    echo "PROTOC HOME ($PROTOC_HOME) does not exist!"
    exit 1
fi

protoc --version
mvn --version
cmake --version
# we need to re-run setup inside the docker container to get mvn-gbn script.
SETUP_FILE="$(mktemp)"
function cleanup_setup_file {
    rm -rf "$SETUP_FILE"
}
trap cleanup_setup_file EXIT

curl https://github.infra.cloudera.com/raw/cdh/cdh/cdh6.x/tools/gerrit-unittest-setup.sh -o "$SETUP_FILE"
source "$SETUP_FILE"

# Build the project
mvn-gbn clean install -Pdist -Pnative -Dtar -Drequire.fuse -Drequire.snappy -DskipTests -Dmaven.javadoc.skip=true

DIR="$( cd $( dirname ${BASH_SOURCE[0]} )  && pwd )"
cd $DIR

# Install dist_test locally
SCRIPTS="dist_test"

if [[ -d $SCRIPTS ]]; then
    echo "Cleaning up remnants from a previous run"
    rm -rf $SCRIPTS
fi

git clone --depth 1 git://github.com/cloudera/$SCRIPTS.git $SCRIPTS || true

# Fetch the right branch
cd "$DIR/$SCRIPTS"
git fetch --depth 1 origin
git checkout -f origin/master
git ls-tree -r HEAD
./setup.sh
export PATH=`pwd`/bin/:$PATH
which grind

if [[ -z $DIST_TEST_USER || -z $DIST_TEST_PASSWORD ]]; then
    # Fetch dist test credentials and add them to the environment
    wget http://staging.jenkins.cloudera.com/gerrit-artifacts/misc/hadoop/dist_test_cred.sh
    source dist_test_cred.sh
fi

# Go to project root
cd "$DIR/.."

# Populate the per-project grind cfg file
cat > .grind_project.cfg << EOF
[grind]
empty_dirs = ["test/data", "test-dir", "log"]
file_globs = []
file_patterns = ["*.so"]
artifact_archive_globs = ["**/surefire-reports/TEST-*.xml"]
EOF

# Invoke grind to run tests
grind -c ${DIR}/$SCRIPTS/env/grind.cfg config
grind -c ${DIR}/$SCRIPTS/env/grind.cfg pconfig

export DIST_TEST_URL_TIMEOUT=180
export GRIND_MAVEN_FLAGS=-Dmaven-dependency-plugin.version=2.10
grind -c ${DIR}/$SCRIPTS/env/grind.cfg test --java-version 8 --artifacts -r 2 \
    -E hadoop-azure \
    -e TestDataNodeUGIProvider \
    -e TestAuditLoggerWithCommands \
    -e TestDistCpUtils \
    -e TestContainerResizing \
    -e TestLeafQueue \
    -e TestTimelineReaderWebServicesHBaseStorage \
    -e TestHBaseStorageFlowActivity \
    -e TestHBaseStorageFlowRun \
    -e TestHBaseStorageFlowRunCompaction \
    -e TestHBaseTimelineStorageApps \
    -e TestHBaseTimelineStorageEntities \
    -e TestHBaseTimelineStorageSchema \
    -e TestRM \
    -e TestWorkPreservingRMRestart \
    -e TestRMRestart \
    -e TestContainerAllocation \
    -e TestMRJobClient \
    -e TestCapacityScheduler \
    -e TestDelegatingInputFormat \
    -e TestMRCJCFileInputFormat \
    -e TestJobHistoryEventHandler \
    -e TestCombineFileInputFormat \
    -e TestAMRMRPCResponseId \
    -e TestSystemMetricsPublisher \
    -e TestNodesListManager \
    -e TestRMContainerImpl \
    -e TestApplicationMasterLauncher \
    -e TestRMWebApp \
    -e TestContainerManagerSecurity \
    -e TestResourceManager \
    -e TestParameterParser \
    -e TestNativeCodeLoader \
    -e TestRMContainerAllocator \
    -e TestMRIntermediateDataEncryption \
    -e TestWebApp \
    -e TestCryptoStreamsWithOpensslAesCtrCryptoCodec \
    -e TestDNS \
    -e TestZKConfigurationStore \
    -e TestApplicationPriority \
    -e TestCapacityOverTimePolicy \
    -e TestPipeApplication \
    -e TestMetricsInvariantChecker \
    -e TestJobMonitorAndPrint \
    -e TestRMWebServicesReservation \
    -e TestNNHandlesBlockReportPerStorage \
    -e TestRMWebServicesCapacitySched \
    -e TestRMWebServicesAppsCustomResourceTypes \
    -e TestRMWebServicesSchedulerActivities \
    -e TestRuntimeEstimators \
    -e TestIncreaseAllocationExpirer


# Cleanup the grind folder
if [[ -d "$DIR/$SCRIPTS" ]]; then
    rm -rf "$DIR/$SCRIPTS"
fi
