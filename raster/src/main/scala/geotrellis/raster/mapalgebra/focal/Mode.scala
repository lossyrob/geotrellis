package geotrellis.raster.mapalgebra.focal

import geotrellis.raster._

/**
 * Computes the mode of a neighborhood for a given raster
 *
 * @note            Mode does not currently support Double raster data.
 *                  If you use a Tile with a Double CellType (FloatConstantNoDataCellType, DoubleConstantNoDataCellType)
 *                  the data values will be rounded to integers.
 */
object Mode {
  def calculation(tile: Tile, n: Neighborhood, bounds: Option[GridBounds] = None, target: TargetCell = TargetCell.All): FocalCalculation[Tile] = {
    n match {
      case Square(ext) => new CellwiseModeCalc(tile, n, bounds, ext, target)
      case _ => new CursorModeCalc(tile, n, bounds, n.extent, target)
    }
  }

  def apply(tile: Tile, n: Neighborhood, bounds: Option[GridBounds] = None, target: TargetCell = TargetCell.All): Tile =
    calculation(tile, n, bounds, target).execute()
}


class CursorModeCalc(r: Tile, n: Neighborhood, bounds: Option[GridBounds], extent: Int, target: TargetCell)
  extends CursorCalculation[Tile](r, n, bounds, target)
  with IntArrayTileResult
  with MedianModeCalculation
{
  initArray(extent)

  def calc(r: Tile, cursor: Cursor) = {
    cursor.removedCells.foreach { (x, y) =>
      val v = r.get(x, y)
      if(isData(v)) {
        removeValue(v)
      }
    }
    cursor.addedCells.foreach { (x, y) =>
      val v = r.get(x, y)
      if(isData(v)) addValue(v)
    }
    resultTile.set(cursor.col, cursor.row, mode)
  }
}


class CellwiseModeCalc(r: Tile, n: Neighborhood, bounds: Option[GridBounds], extent: Int, target: TargetCell)
  extends CellwiseCalculation[Tile](r, n, bounds, target)
  with IntArrayTileResult
  with MedianModeCalculation
{
  initArray(extent)

  def add(r: Tile, x: Int, y: Int) = {
    val v = r.get(x, y)
    if (isData(v)) {
      addValue(v)
    }
  }

  def remove(r: Tile, x: Int, y: Int) = {
    val v = r.get(x, y)
    if (isData(v)) {
      removeValue(v)
    }
  }

  def setValue(x: Int, y: Int) = {
    resultTile.setDouble(x, y, mode)
  }
}
