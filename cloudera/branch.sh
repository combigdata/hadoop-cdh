#!/bin/bash

set -exu
mvn --batch-mode versions:set -DnewVersion=3.0.0-cdh$CDH_NEW_MAVEN_VERSION
sed -i "s/$CDH_START_VERSION/$CDH_NEW_VERSION/" cloudera/lib.sh cloudera/inside-docker.sh
# Update the parent version. mvn versions:update-parent is useless since new_maven_version may not exist yet
perl -0777 -i -pe 's/(<version>).*(<\/version>\s*\n*<\/parent>)/\1$ENV{CDH_NEW_MAVEN_VERSION}\2/' pom.xml

# Assert something changed and print the diff.
! git diff --exit-code
# Stage changes
git add -u
