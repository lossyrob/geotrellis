package geotrellis.proj4.proj

import geotrellis.proj4.{ProjCoordinate, ProjectionException}
import geotrellis.util.ProjectionMath._

object AiryProjection {

  final val NPole = 0

  final val SPole = 1

  final val Equit = 2

  final val Obliq = 3
}

object AiryProjectionBuilder extends ProjectionBuilder {
  def apply(pp: ProjectionParams): Projection = {

  import AiryProjection._

  val beta = 0.5 * math.Pi / 2

  val cb = 1 / math.tan(beta) * math.log(math.cos(beta))

  val (halfPi, mode, sinPh, cosPh) =
    if (math.abs(math.abs(pb.projectionLatitude) - math.Pi / 2) < EPS_10) {
      if (pb.projectionLatitude < 0) (-math.Pi / 2, SPole, 0.0, 0.0)
      else  (math.Pi / 2, NPole, 0.0, 0.0)
    } else {
      if (math.abs(pb.projectionLatitude) < EPS_1) (0.0, Equit, 0.0, 0.0)
      else (0.0, Obliq, math.sin(pb.projectionLatitude), math.cos(pb.projectionLatitude))
    }

  override def project(lplam: Double, lpphi: Double): ProjCoordinate = {
    var sinLam, cosLam, cosPhi, sinPhi, t, s, krho, cosz = 0.0

    sinLam = math.sin(lplam)
    cosLam = math.cos(lplam)

    mode match {
      case Equit | Obliq => {
        sinPhi = math.sin(lpphi)
        cosPhi = math.cos(lpphi)
        cosz = cosPhi * cosLam

        if (mode == Obliq) cosz = sinPh * sinPhi + cosPh * cosPhi
        if (cosz < EPS_10) throw new ProjectionException("F")

        s = 1 - cosz

        if (math.abs(s) < EPS_10) {
          t = 0.5 * (1 + cosz)
          krho = -math.log(t) / s - cb / t
        } else krho = 0.5 - cb

        val x = krho * cosPhi * sinLam
        val y =
          if (mode == Obliq) krho * (cosPh * sinPhi - sinPh * cosPhi * cosLam)
          else krho * sinPhi

        ProjCoordinate(x, y)
      }
      case NPole | SPole => {
        if (lpphi - EPS_10 > math.Pi / 2) throw new ProjectionException("F")

        var y = math.abs(halfPi - lpphi) * 0.5
        if (y > EPS_10) {
          t = math.tan(lpphi)
          krho = -2 * (math.log(math.cos(lpphi)) / t + t * cb)
          val x = krho * sinLam
          y = krho * cosLam

          if (mode == NPole) y = -y
          ProjCoordinate(x, y)
        } else ProjCoordinate(0, 0)
      }
    }
  }
}
