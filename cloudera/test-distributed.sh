#!/bin/bash
set -xe

# activate mvn-gbn wrapper
# mv "$(which mvn-gbn-wrapper)" "$(dirname "$(which mvn-gbn-wrapper)")/mvn"

# DIR="$( cd $( dirname ${BASH_SOURCE[0]} )  && pwd )"
# cd $DIR


# invoke docker with an image that can build hadoop
# namely we need: cmake 3.1+, mvn 3.3+, protobuf2.5+, java 8.
# toolchain is currently used to conveniently setup most of these.
docker run -v "$(pwd):/hadoop" -w /hadoop \
    -e PRODUCT_BRANCH \
    docker-registry.infra.cloudera.com/cauldron/ubuntu1604:1505254507 \
    bash -c "adduser --uid $(id -u) --gecos '' --disabled-password pre_post_commit; su pre_post_commit -c '/hadoop/cloudera/inside-docker.sh'"