#!/usr/bin/env bash

# Set the first argument to the scala version

set -e
set -x

./sbt "++@1" "project macros" publish \
  && ./sbt "++@1" "project vector" publish \
  && ./sbt "++@1" "project proj4" publish \
  && ./sbt "++@1" "project raster" publish \
  && ./sbt "++@1" "project spark" publish \
  && ./sbt "++@1" "project s3" publish \
  && ./sbt "++@1" "project accumulo" publish \
  && ./sbt "++@1" "project cassandra" publish \
  && ./sbt "++@1" "project hbase" publish \
  && ./sbt "++@1" "project spark-etl" publish \
  && ./sbt "++@1" "project geomesa" publish \
  && ./sbt "++@1" "project geotools" publish \
  && ./sbt "++@1" "project geowave" publish \
  && ./sbt "++@1" "project shapefile" publish \
  && ./sbt "++@1" "project slick" publish \
  && ./sbt "++@1" "project util" publish \
  && ./sbt "++@1" "project raster-testkit" publish \
  && ./sbt "++@1" "project vector-testkit" publish \
  && ./sbt "++@1" "project spark-testkit" publish
