package geotrellis.raster.render.png

import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.raster.testkit._

import spire.syntax.cfor._
import org.scalatest._

import java.io._
import javax.imageio._

class RenderPngTests extends FunSuite with Matchers with TileBuilders with RasterMatchers {
  def testPng(png: Png, tile: Tile, colorMap: ColorMap): Unit = {
    val img = ImageIO.read(new ByteArrayInputStream(png.bytes))

    img.getWidth should be (tile.cols)
    img.getHeight should be (tile.rows)

    cfor(0)(_ < img.getWidth, _ + 1) { col =>
      cfor(0)(_ < img.getHeight, _ + 1) { row =>
        val argb = img.getRGB(col, row)
        val actual = (argb << 8) | ((argb >> 24) & 0xFF)
        val expected = colorMap.map(tile.get(col, row))

        withClue(f"$actual%02X does not equal $expected%02X") {
          actual should be (expected)
        }
      }
    }
  }

  def createConsecutiveTile(d: Int, start: Int): Tile = {
    val tile = ArrayTile.empty(IntCellType, d, d)
    var i = start
    cfor(0)(_ < d, _ + 1) { row =>
      cfor(0)(_ < d, _ + 1) { col =>
        tile.set(col, row, i)
        i += 1
      }
    }
    tile
  }

  test("should render a PNG and match what is read in by ImageIO when written as Indexed") {
    val tileNW =
      createValueTile(50, 1)
    val tileNE =
      createValueTile(50, 2)
    val tileSW =
      createValueTile(50, 3)
    val tileSE =
      createValueTile(50, 4)

    val tile =
      CompositeTile(Seq(tileNW, tileNE, tileSW, tileSE), TileLayout(2, 2, 50, 50)).toArrayTile

    val colorMap =
      ColorMap(
        Map(
          1 -> RGBA(255, 0, 0, 255).int,
          2 -> RGBA(0, 255, 0, 255).int,
          3 -> RGBA(0, 0, 255, 255).int,
          4 -> RGBA(0, 255, 255, 0xBB).int
        )
      )

    val png = tile.renderPng(colorMap)

    testPng(png, tile, colorMap)
  }

  test("should render a PNG from an Int tile and match what is read in by ImageIO when written as Indexed with nodata values") {
    val tileNW =
      createValueTile(50, 1)
    val tileNE =
      createValueTile(50, 2)
    val tileSW =
      createValueTile(50, 3)
    val tileSE =
      createValueTile(50, NODATA)

    val tile =
      CompositeTile(Seq(tileNW, tileNE, tileSW, tileSE), TileLayout(2, 2, 50, 50)).toArrayTile

    val colorMap =
      ColorMap(
        Map(
          1 -> RGBA(255, 0, 0, 255).int,
          2 -> RGBA(0, 255, 0, 255).int,
          3 -> RGBA(0, 0, 255, 255).int,
          4 -> RGBA(0, 255, 255, 0xBB).int
        )
      ).withNoDataColor(0xFFFFFFAA)

    val png = tile.renderPng(colorMap)

    testPng(png, tile, colorMap)
  }

  test("should render a PNG from a Double tile and match what is read in by ImageIO when written as Indexed with nodata values") {
    val tileNW =
      createValueTile(50, 1)
    val tileNE =
      createValueTile(50, 2)
    val tileSW =
      createValueTile(50, 3)
    val tileSE =
      createValueTile(50, NODATA)

    val tile =
      CompositeTile(Seq(tileNW, tileNE, tileSW, tileSE), TileLayout(2, 2, 50, 50))
        .convert(DoubleConstantNoDataCellType)
        .toArrayTile

    val colorMap =
      ColorMap(
        Map(
          1.0 -> RGBA(255, 0, 0, 255).int,
          2.0 -> RGBA(0, 255, 0, 255).int,
          3.0 -> RGBA(0, 0, 255, 255).int,
          4.0 -> RGBA(0, 255, 255, 0xBB).int
        )
      ).withNoDataColor(0xFFFFFFAA)

    val png = tile.renderPng(colorMap)

    testPng(png, tile, colorMap)
  }

  test("render int and double tiles similarly") {
    val tileNW =
      createValueTile(50, 1)
    val tileNE =
      createValueTile(50, 2)
    val tileSW =
      createValueTile(50, 3)
    val tileSE =
      createValueTile(50, 4)

    val intTile =
      CompositeTile(Seq(tileNW, tileNE, tileSW, tileSE), TileLayout(2, 2, 50, 50)).toArrayTile

    val doubleTile =
      CompositeTile(Seq(tileNW, tileNE, tileSW, tileSE), TileLayout(2, 2, 50, 50))
        .convert(DoubleConstantNoDataCellType)
        .toArrayTile


    val intColorMap =
      ColorMap(
        Map(
          1 -> RGB(255, 0, 0).int,
          2 -> RGB(0, 255, 0).int,
          3 -> RGB(0, 0, 255).int,
          4 -> RGB(0, 255, 255).int
        )
      )

    val doubleColorMap =
      ColorMap(
        Map(
          1.0 -> RGB(255, 0, 0).int,
          2.0 -> RGB(0, 255, 0).int,
          3.0 -> RGB(0, 0, 255).int,
          4.0 -> RGB(0, 255, 255).int
        )
    )

    val intPng = intTile.renderPng(intColorMap)
    val doublePng = doubleTile.renderPng(doubleColorMap)

    val intImg = ImageIO.read(new ByteArrayInputStream(intPng.bytes))
    val doubleImg = ImageIO.read(new ByteArrayInputStream(doublePng.bytes))

    cfor(0)(_ < intImg.getWidth, _ + 1) { col =>
      cfor(0)(_ < intImg.getHeight, _ + 1) { row =>
        intImg.getRGB(col, row) should be (doubleImg.getRGB(col, row))
      }
    }
  }

  test("should render a PNG and match what is read in by ImageIO when written as RGBA") {
    val tileNW =
      createConsecutiveTile(50, 1)
    val tileNE =
      createConsecutiveTile(50, 2501)
    val tileSW =
      createConsecutiveTile(50, 5001)
    val tileSE =
      createConsecutiveTile(50, 7501)

    val tile =
      CompositeTile(Seq(tileNW, tileNE, tileSW, tileSE), TileLayout(2, 2, 50, 50)).toArrayTile

    val colorMap =
      ColorRamp(0xFF0000FF, 0x0000FFFF)
        .stops(1000)
        .setAlphaGradient(0xFF, 0xAA)
        .toColorMap(tile.histogram)

    val png = tile.renderPng(colorMap)

    testPng(png, tile, colorMap)
  }

  test("should render a PNG and match what is read in by ImageIO when written as RGBA with nodata values") {
    val tileNW =
      createConsecutiveTile(50, 1)
    val tileNE =
      createConsecutiveTile(50, 2501)
    val tileSW =
      createConsecutiveTile(50, 5001)
    val tileSE =
      createValueTile(50, NODATA)

    val tile =
      CompositeTile(Seq(tileNW, tileNE, tileSW, tileSE), TileLayout(2, 2, 50, 50)).toArrayTile

    val colorMap =
      ColorRamp(0xFF0000FF, 0x0000FFFF)
        .stops(1000)
        .setAlphaGradient(0xFF, 0xAA)
        .toColorMap(tile.histogram)
        .withNoDataColor(0xFFFFFFAA)

    val png = tile.renderPng(colorMap)

    testPng(png, tile, colorMap)
  }

  test("png encoding produces the same colors for indexed and RGBA") {
    val pngIndexedTmpFile = File.createTempFile("png-indexed-img", ".png")
    val pngRGBATmpFile = File.createTempFile("png-rgba-img", ".png")

    val tile: IntArrayTile = IntArrayTile(1 to 256*256 toArray, 256, 256)
    val ramp = ColorRamp(0xff0000ff, 0x0000ffff)  // red to blue
    val stops = Array(10000, 20000, 30000, 40000, 50000, 60000, 70000)
    val colorMap = ColorMap(stops, ramp)

    val indexedPng = tile.renderPng(colorMap)
    val rgbaPng = colorMap.render(tile).renderPng()

    val indexedImg = ImageIO.read(new ByteArrayInputStream(indexedPng.bytes))
    val rgbaImg = ImageIO.read(new ByteArrayInputStream(rgbaPng))

    cfor(0)(_ < tile.rows, _ + 1) { row =>
      cfor(0)(_ < tile.cols, _ + 1) { col =>
        val actual = indexedImg.getRGB(col, row)
        val expected = rgbaImg.getRGB(col, row)
        withClue(f"$actual%02X does not equal $expected%02X") {
          actual should be (expected)
        }
      }
    }
  }
}
