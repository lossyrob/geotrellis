package geotrellis.run

import com.beust.jcommander._

import geotrellis._
import geotrellis.data._
import geotrellis.process._
import geotrellis.raster.op._
import geotrellis.io

/**
 * Task that diffs two rasters.
 *
 * The diff output provides information about how the two rasters differ.
 */
@Parameters(commandNames = Array("diff"), commandDescription ="Provides diff information between two rasters.")
class DiffTask extends Task { 
  @Parameter( 
    names = Array("--source", "-s"),  
    description = "Path of source ARG raster (to be diffed against)",  
    required=true) 
  var sourcePath:String = _  
 
  @Parameter( 
    names = Array("--target", "-t"),   
    description = "Path of target ARG raster (to diff against source)",
    required=true) 
  var targetPath:String = _ 
 
  val taskName = "diff"
 
  def execute = {
    DiffTask.execute(sourcePath, targetPath)
  } 
} 

object DiffTask {
  def doesFileExist(p:String) = {
    if(!(new java.io.File(p).exists())) {
      println("File %s does not exist.".format(p))
      false
    }
    true
  }

  def execute(sourcePath:String, targetPath:String):Unit = {  
    if(!doesFileExist(sourcePath)) { return }
    if(!doesFileExist(targetPath)) { return }    

    val localServer = TestServer()
    
    try {
      val r1Op = io.LoadFile(sourcePath)
      val r2Op = io.LoadFile(targetPath)
      val diffOp = local.Subtract(r1Op, r2Op)
      val diff = localServer.run(diffOp)

      val arr = diff.data.asArray.get.toArrayDouble
      val differences = arr.filter(i => i != 0 )
      val (max, min) = (arr.max, arr.min)

      println("There are %d difference(s) between these rasters.".format(differences.length))
      println("The range of differences is (%f, %f)".format(min, max))
    } finally {
      localServer.shutdown()
    }
  }
}
