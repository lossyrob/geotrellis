package geotrellis.network.graph

sealed abstract class EdgeType

case object WalkEdge extends EdgeType {
  def apply(target:Vertex,time:Time,travelTime:Duration) = 
    Edge(target,time,travelTime,WalkEdge)
}

case object TransitEdge extends EdgeType {
  def apply(target:Vertex,time:Time,travelTime:Duration) = 
    Edge(target,time,travelTime,TransitEdge)
}

case object BikeEdge extends EdgeType {
  def apply(target:Vertex,time:Time,travelTime:Duration) = 
    Edge(target,time,travelTime,BikeEdge)
}

case class Edge(target:Vertex,time:Time,travelTime:Duration,edgeType:EdgeType) {
  def isAnyTime = travelTime.isAny
}
