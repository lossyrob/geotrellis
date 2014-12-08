package geotrellis.proj4.proj

import geotrellis.proj4.{ProjCoordinate, ProjectionException}
import geotrellis.proj4.util.ProjectionMath._

object AlbersProjectionBuilder extends ProjectionBuilder {
  private val Tol7 = 1e-7
  private val Iters = 15
  private val Eps = 1e-7
  private val Tol = 1e-10

  val epsg = 9822
  val name = "Albers Equal Area"

  def apply(params: ProjectionParams): Projection = {
    val phi1 = pb.projectionLatitude1

    val phi2 = pb.projectionLatitude2

    if (math.abs(phi1 + phi2) < EPS_10) 
      throw new ProjectionException("-21: Projection Latitude parameters 1 and 2 have a sum of approximately zero.")

    val sinPhi = math.sin(phi1)

    val cosPhi = math.cos(phi1)

    val secant = math.abs(phi1 - phi2) >= EPS_10

    val (n, ec, c, dd, rho) =
      if (spherical) {
        val n = if (secant) 0.5 * (sinPhi + math.sin(phi2)) else sinPhi
        val ec = 0.0
        val c = cosPhi * cosPhi + (n + n) * sinPhi
        val dd = 1 / n
        val rho = dd * math.sqrt(c - (n + n) * math.sin(pb.projectionLatitude))
        (n, ec, c, dd, rho)
      } else {
        if (enfn(es) == null) throw new ProjectionException("0") // TODO: What is this failure case?
        val m1 = msfn(sinPhi, cosPhi, es)
        val ml1 = qsfn(sinPhi, e, oneEs)

        val n = if (secant) {
          val sp = math.sin(phi2)
          val cp = math.cos(phi2)

          val m2 = msfn(sp, cp, es)
          val ml2 = qsfn(sp, e, oneEs)

          (m1 * m1 - m2 * m2) / (ml2 - ml1)
        } else sinPhi

        val ec = 1 - 0.5 * oneEs * math.log((1 - e) / (1 + e)) / e
        val c = m1 * m1 + n * ml1
        val dd = 1 / n
        val rho = dd * math.sqrt(c - n * qsfn
          (
            math.sin(pb.projectionLatitude),
            e,
            oneEs
          )
        )

        (n, ec, c, dd, rho)
      }

    def phi1(qs: Double, te: Double, toneEs: Double) = {
      var i = 0
      var phi, sinPi, cosPi, con, com, dPhi = 0.0

      phi = math.asin(0.5 * qs)

      if (te < Eps) phi
      else {
        i = Iters
        while (i > 0 && math.abs(dPhi) > Tol) {
          sinPi = math.sin(phi)
          cosPi = math.cos(phi)
          con = te * sinPi
          com = 1 - con * con
          dPhi = 0.5 * com * com / cosPi * (qs / toneEs -
            sinPi / com + 0.5 / te * math.log((1 - con) / (1 + con)))

          phi += dPhi
          i -= 1
        }

        if (i != 0) phi else Double.MaxValue
      }
    }


    def project(lplam: Double, lpphi: Double): (Double, Double) = {
      var r = c - {
        if (spherical) { (n + n) * math.sin(lpphi) }
        else { n * qsfn(math.sin(lpphi), e, oneEs) }
      }

      if (r < 0) throw new ProjectionException("F") // TODO: What is this error case?

      r = dd * math.sqrt(r)

      val nLplam = lplam * n
      val x = r * math.sin(nLplam)
      val y = rho - r * math.cos(nLplam)

      (x, y)
    }

    def projectInverse(xyx: Double, xyy: Double): (Double, Double) = {
      val rhoXyy = rho - xyy
      var r = distance(xyx, rhoXyy)
      if (r != 0) {

        var yx = xyx
        var yy = xyy

        if (n < 0) {
          r = -r
          yx = -yx
          yy = yy
        }

        var lpphi = rho / dd

        if (!spherical) {
          lpphi = (c - lpphi * lpphi) / n

          if (math.abs(ec - math.abs(lpphi)) > Tol7) {
            lpphi = phi1(lpphi, e, oneEs)
            if (lpphi == Double.MaxValue) throw new ProjectionException("I")
          } else lpphi = if (lpphi < 0) -math.Pi / 2 else math.Pi / 2
        } else if (math.abs(c - lpphi * lpphi) / (n + n) <= 1)
          lpphi = math.asin(lpphi)
        else lpphi = if (lpphi < 0) -math.Pi / 2 else math.Pi / 2

        val lplam = math.atan2(yx, yy) / 2

        (lplam, lpphi)
      } else (0, if (n > 0) math.Pi / 2 else -math.Pi / 2)
    }
  }
}
