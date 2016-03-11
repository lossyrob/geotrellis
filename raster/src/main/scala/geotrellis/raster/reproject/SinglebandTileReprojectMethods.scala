package geotrellis.raster.reproject

import geotrellis.raster._
import geotrellis.proj4._
import geotrellis.vector.Extent

trait SinglebandTileReprojectMethods extends TileReprojectMethods[Tile] {
  import Reproject.Options

  def reproject(srcExtent: Extent, targetRasterExtent: RasterExtent, transform: Transform, inverseTransform: Transform, options: Options): Raster[Tile] =
    Raster(self, srcExtent).reproject(targetRasterExtent, transform, inverseTransform, options)

  def reproject(srcExtent: Extent, src: CRS, dest: CRS, options: Options): Raster[Tile] =
    Raster(self, srcExtent).reproject(src, dest, options)

  def reproject(srcExtent: Extent, gridBounds: GridBounds, src: CRS, dest: CRS, options: Options): Raster[Tile] =
    Raster(self, srcExtent).reproject(gridBounds, src, dest, options)

  def reproject(srcExtent: Extent, gridBounds: GridBounds, transform: Transform, inverseTransform: Transform, options: Options): Raster[Tile] =
    Raster(self, srcExtent).reproject(gridBounds, transform, inverseTransform, options)
}
