package geotrellis.raster

import geotrellis._
import geotrellis.util.Filesystem
import geotrellis.process._
import geotrellis.data.arg.{ArgWriter,ArgReader}
import geotrellis.feature.Polygon
import java.io.{FileOutputStream, BufferedOutputStream}
import geotrellis.util.Filesystem

/**
 * Used to create tiled rasters, as well as tilesets on the filesystem, based
 * on a source raster.
 *
 * These files (on disk) can be used by a TileSetRasterData, or loaded into an
 * array of rasters to be used by TileArrayRasterData.
 *
 * A tile set has a base path (e.g. "foo/bar") which is used along with the
 * "tile coordinates" (e.g. tile 0,4) to compute the path of each tile (in this
 * case "foo/bar_0_4.arg").
 */
object Tiler {
  /**
    * Given a name ("bar") a col (0), and a row (4), returns the correct name
    * for this tile ("bar_0_4").
    */
  private def tileName(name:String, col:Int, row:Int) = {
    "%s_%d_%d".format(name, col, row)
  }

  /**
    * Given a path ("foo"), a name ("bar"), a col (0), and a row (4), returns
    * the correct name for this tile ("foo/bar_0_4").
    */
  def tilePath(path:String, name:String, col:Int, row:Int) = {
    Filesystem.join(path, tileName(name, col, row) + ".arg")
  }
}
