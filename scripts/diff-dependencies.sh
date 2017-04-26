#!/bin/bash

set -e

if [[ -n "${PC_DEMO_DEBUG}" ]]; then
    set -x
fi

function usage() {
    echo -n \
         "Usage: $(basename "$0") NEW_RELEASE_BRANCH PREVIOUS_RELEASE_BRANCH
Show diffs to look for changes in dependencies (for making CQ's between versions).
"
}

if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    if [ "${1:-}" = "--help" ] || [ "${1}" = "" ] || [ "${2}" = "" ]; then
        usage
    else
        OLD_BRANCH="${1}"
        NEW_BRANCH="${2}"

        echo "git diff ${OLD_BRANCH} ${NEW_BRANCH} -- project/Dependencies.scala"

        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- project/Dependencies.scala
        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- project/Version.scala

        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- macros/build.sbt
        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- vector/build.sbt
        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- proj4/build.sbt
        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- raster/build.sbt
        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- spark/build.sbt
        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- s3/build.sbt
        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- accumulo/build.sbt
        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- cassandra/build.sbt
        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- hbase/build.sbt
        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- spark-etl/build.sbt
        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- geomesa/build.sbt
        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- geowave/build.sbt
        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- geotools/build.sbt
        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- shapefile/build.sbt
        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- slick/build.sbt
        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- util/build.sbt
        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- vectortile/build.sbt
        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- raster-testkit/build.sbt
        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- vector-testkit/build.sbt
        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- spark-testkit/build.sbt
        git diff ${OLD_BRANCH} ${NEW_BRANCH} -- s3-testkit/build.sbt

    fi
fi
