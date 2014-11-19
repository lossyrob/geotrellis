package climate.utils

import org.apache.spark.SparkContext

object Utils {
  def addClimateJar(sc: SparkContext): Unit = {
    scala.util.Properties.envOrNone("CLIMATE_HOME") match {
      case Some(jar) => sc.addJar(jar)
      case None => sys.error("Could not find CLIMATE_HOME enviroment variable.")
    }
  }
}