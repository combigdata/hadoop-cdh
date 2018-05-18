#!/bin/bash
# (c) Copyright (2018) Cloudera, Inc.

set -xe

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

. $DIR/lib.sh

export MAVEN_FLAGS="-P IntegrationsTest"
main $@
