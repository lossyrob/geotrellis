/*
 * Copyright (c) 2014 Azavea.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.raster

import geotrellis.raster.resample._
import geotrellis.vector.Extent

import spire.syntax.cfor._

/**
 * LazyConvertedTile represents a lazily-applied conversion to any type.
 *
 * @note     If you care converting to a CellType with less bits
 *           than the type of the underlying data, you are responsible
 *           for managing overflow. This convert does not do any casting;
 *           therefore converting from a TypeInt to TypeByte could still
 *           return values greater than 127 from apply().
 */
final case class LazyConvertedArrayTile(inner: Tile, cellType: CellType)
  extends Tile {

  val cols = inner.cols
  val rows = inner.rows

  def convert(cellType: CellType): Tile =
    LazyConvertedArrayTile(this, cellType)

  override def toArray = inner.toArray
  override def toArrayDouble = inner.toArrayDouble

  def get(col: Int, row: Int): Int = 
    inner.get(col, row)

  def getDouble(col: Int, row: Int): Double =
    inner.getDouble(col, row)

  def toArrayTile: ArrayTile = mutable

  def mutable: MutableArrayTile = {
    val tile = ArrayTile.alloc(cellType, cols, rows)

    if(!cellType.isFloatingPoint) {
      cfor(0)(_ < rows, _ + 1) { row =>
        cfor(0)(_ < cols, _ + 1) { col =>
          tile.set(col, row, get(col, row))
        }
      }
    } else {
      cfor(0)(_ < rows, _ + 1) { row =>
        cfor(0)(_ < cols, _ + 1) { col =>
          tile.setDouble(col, row, getDouble(col, row))
        }
      }
    }

    tile
  }

  def toBytes(): Array[Byte] = toArrayTile.toBytes

  def foreach(f: Int => Unit): Unit = inner.foreach(f)
  def foreachDouble(f: Double => Unit): Unit = inner.foreachDouble(f)

  def foreachIntVisitor(visitor: IntTileVisitor): Unit = inner.foreachIntVisitor(visitor)
  def foreachDoubleVisitor(visitor: DoubleTileVisitor): Unit = inner.foreachDoubleVisitor(visitor)

  def map(f: Int => Int): Tile = {
    val tile = ArrayTile.alloc(cellType, cols, rows)

    cfor(0)(_ < rows, _ + 1) { row =>
      cfor(0)(_ < cols, _ + 1) { col =>
        tile.set(col, row, f(get(col, row)))
      }
    }

    tile
  }

  def mapDouble(f: Double =>Double): Tile = {
    val tile = ArrayTile.alloc(cellType, cols, rows)

    cfor(0)(_ < rows, _ + 1) { row =>
      cfor(0)(_ < cols, _ + 1) { col =>
        tile.setDouble(col, row, f(getDouble(col, row)))
      }
    }

    tile
  }

  def mapIntMapper(mapper: IntTileMapper): Tile = {
    val tile = ArrayTile.alloc(cellType, cols, rows)

    cfor(0)(_ < rows, _ + 1) { row =>
      cfor(0)(_ < cols, _ + 1) { col =>
        tile.set(col, row, mapper(col, row, get(col, row)))
      }
    }

    tile
  }

  def mapDoubleMapper(mapper: DoubleTileMapper): Tile = {
    val tile = ArrayTile.alloc(cellType, cols, rows)

    cfor(0)(_ < rows, _ + 1) { row =>
      cfor(0)(_ < cols, _ + 1) { col =>
        tile.setDouble(col, row, mapper(col, row, getDouble(col, row)))
      }
    }

    tile
  }


  def combine(other: Tile)(f: (Int, Int) => Int): Tile = {
    (this, other).assertEqualDimensions

    val tile = ArrayTile.alloc(cellType, cols, rows)
    cfor(0)(_ < rows, _ + 1) { row =>
      cfor(0)(_ < cols, _ + 1) { col =>
        tile.set(col, row, f(get(col, row), other.get(col, row)))
      }
    }

    tile
  }

  def combineDouble(other: Tile)(f: (Double, Double) => Double): Tile = {
    (this, other).assertEqualDimensions

    val tile = ArrayTile.alloc(cellType, cols, rows)
    cfor(0)(_ < rows, _ + 1) { row =>
      cfor(0)(_ < cols, _ + 1) { col =>
        tile.setDouble(col, row, f(getDouble(col, row), other.getDouble(col, row)))
      }
    }

    tile
  }
}
