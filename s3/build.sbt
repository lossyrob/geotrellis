import Dependencies._

name := "geotrellis-s3"
libraryDependencies ++= Seq(
  sparkCore % "provided",
  pdal,
  awsSdkS3,
  spire,
  scalatest % "test")

resolvers += Resolver.bintrayRepo("daunnc", "maven")

fork in Test := false
parallelExecution in Test := false

initialCommands in console :=
  """
  import geotrellis.raster._
  import geotrellis.vector._
  import geotrellis.proj4._
  import geotrellis.spark._
  import geotrellis.spark.util._
  import geotrellis.spark.tiling._
  """
