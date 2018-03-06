#!/bin/bash
set -exu
mvn versions:set -DnewVersion=3.0.0-cdh$CDH_NEW_MAVEN_VERSION
sed -i "s/$CDH_START_VERSION/$CDH_NEW_VERSION/" cloudera/lib.sh

# Assert something changed and print the diff.
! git diff --exit-code
# Stage changes
git add -u
