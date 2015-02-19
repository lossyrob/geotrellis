import org.scalatest._

import java.io._

import geotrellis.raster.io.geotiff.reader._

class FINDTHEGEOTIFF extends FunSuite with Matchers {
  /** The directory that contains the GeoTiff files. 
    * Change to the directory that contain the GeoTiffs we want to test */
  val THE_DIR = "/Users/rob/data/nlcd/clipped_tiles"

  def gatherTiffs(f: File): Array[File] = {
    val fileList = f.listFiles
    val tiffs = fileList.filter { f => f.getName.endsWith(".tif") || f.getName.endsWith(".tiff") }
    val dirs = fileList.filter(_.isDirectory)
    tiffs ++ dirs.flatMap(gatherTiffs)
  }

  test("Let's play find the GeoTiff!") {
    val dir = new File(THE_DIR)

    if(!dir.exists)
      sys.error(s"PATH $dir DOES NOT EXIST!!")

    if(!dir.isDirectory)
      sys.error(s"PATH $dir IS NOT A DIRECTORY!!")

    val tiffs = gatherTiffs(dir)

    if(tiffs.isEmpty)
      sys.error("PATH $dir DOESN'T HAVE ANY TIFFS!")

    var badCount = 0

    for(tiffFile <- tiffs) {
      try {
        GeoTiff(tiffFile.getAbsolutePath)
      } catch {
        case e: Exception =>
          println(s"=======FOUND ONE! $tiffFile========")
          badCount += 1
      }
    }

    println(s"---There were $badCount GeoTiff files our reader could not read.---")
    withClue(s"Check above to find the paths of the $badCount unreadable GeoTiffs.") { assert(badCount == 0) }
  }
  
}
