package geotrellis.data

import geotrellis._

import geotrellis.logic.Collect

import geotrellis.raster.{TileLayout,TileArrayRasterData}
import geotrellis.raster.op._

import scala.collection.mutable

object DistributedRasterSource {
  def apply(name:String):DistributedRasterSource =
    new DistributedRasterSource(
      io.LoadRasterLayerInfo(name).map { info =>
        RasterDefinition(
          info.rasterExtent,
          info.tileLayout,
          (for(tileCol <- 0 until info.tileLayout.tileCols;
            tileRow <- 0 until info.tileLayout.tileRows) yield {
            io.LoadTile(name,tileCol,tileRow)
          }).toSeq
        )
      }
    )
}


trait RasterSource {
  def tiles:Op[Seq[Op[Raster]]]
}

class LocalRasterSource(val raster:Op[Raster]) {
  def tiles = raster.map(_.getTileOps)
}


// UnboundRaster
case class RasterDefinition(re:RasterExtent,tileLayout:TileLayout,tiles:Seq[Op[Raster]]) {
  def mapTiles(f:Op[Raster]=>Op[Raster]) = 
    RasterDefinition(re,tileLayout,tiles.map(f(_)))
}

// BoundRaster
// Raster(rasterData,re)

class DistributedRasterSource(val rasterDef:Op[RasterDefinition]) {
  def tiles = rasterDef.map(_.tiles)

  def converge = rasterDef.map { rd =>
    val re = rd.re
    Collect(rd.tiles).map(s => Raster(TileArrayRasterData(s.toArray,rd.tileLayout,re), re))
  }

  def add(c:Int):DistributedRasterSource =
    new DistributedRasterSource(rasterDef.map(_.mapTiles(local.Add(_,c))))

  def add(ds:DistributedRasterSource):DistributedRasterSource =
    new DistributedRasterSource(
      for(thisDef <- rasterDef;
          thatDef <- ds.rasterDef) yield {
        if(thisDef.tileLayout != thatDef.tileLayout) {
          sys.error("Tile layouts do not match.")
        }

        if(thisDef.re != thatDef.re) {
          sys.error("RasterExtents do not match.")
        }
        RasterDefinition(
          thisDef.re,
          thisDef.tileLayout,
          thisDef.tiles.zip(thatDef.tiles).map { case (t1,t2) =>
            local.Add(t1,t2)
          }
        )
      }
    )
}

class LocalRasterSource(val rasterDef:Op[Raster]) {
  def tiles = raster.map(_.getTileOps)

  def converge = rasterDef.map { rd =>
    val re = rd.re
    Collect(rd.tiles).map(s => Raster(TileArrayRasterData(s.toArray,rd.tileLayout,re), re))
  }

  def costDistance(points:Op[Seq[(Int,Int)]]) =
    //new LocalRasterSource( converge.map(focal.CostDistance(_,points))) 



      rasterDef.flatMap { rd =>
        val result = converge.map(focal.CostDistance(_,points)
        result.map( r =>
        RasterDefinition(r.rasterExtent,
                         r.tileLayout,
                         r)
        )
    })

  def add(c:Int):LocalRasterSource =
    new LocalRasterSource( MapOnTiles(tiles, Add(_,c)) )

    tileLoadingOp: Op[Seq[Op[Raster]]]

    tileLoadingOp.map( seq =>
      seq.map ( Add(_, 3) )
    )

    Add(Add(GetTileOps(r), 3))
  
    r.mapOnTiles(Add(_, 3)).mapOnTiles(Add(_,3))




    // Distributed
    Seq( Add(Add(Add(io.LoadTile("thing",3,4),3),3),3),
         Add(Add(Add(io.LoadTile("thing",3,5),3),3),3),
         Add(Add(Add(io.LoadTile("thing",3,6),3),3),3))

  |> Converge(_)
  |>
  // Local
  Seq( Add(Add(Add_.map(r => r.getTile(3,4)),3),3),3),
       Add(Add(Add_.map(r => r.getTile(3,5)),3),3),3),
       Add(Add(Add_.map(r => r.getTile(3,6)),3),3),3))
      

////
     Seq(
       Add(io.LoadTile("thing",3,4),3),
       Add(io.LoadTile("thing",3,5),3),
       Add(io.LoadTile("thing",3,6),3)
     )
}


object Foo {
  val ds1 = DistributedRasterSource("fooraster")
  val ds2 = DistributedRasterSource("fooraster")
  val ds3:DistributedRasterSource = ds1.add(ds2)
}
