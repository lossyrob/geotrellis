package geotrellis.network.graph

case class Edge(target:Vertex,time:Time,travelTime:Duration) {
  def isAnyTime = travelTime.isAny
}
