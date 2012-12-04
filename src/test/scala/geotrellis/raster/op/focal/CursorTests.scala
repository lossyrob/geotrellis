package geotrellis.raster.op.focal

import geotrellis._

import scala.collection.mutable.Set
import scala.math._

import org.scalatest.FunSpec
import org.scalatest.ShouldMatchers

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class CursorSpec extends FunSpec with ShouldMatchers {
  val foldLeftCB = new IntFocalFoldCB { def act(a:Int,v:Int) = a + v }
  val foldLeftMinCB = new IntFocalFoldCB { def act(a:Int,v:Int) = min(a,v) }

  def createRaster:Raster = {
    val arr = (for(i <- 1 to 100) yield i).toArray
    Raster(arr, RasterExtent(Extent(0,0,10,10),1,1,10,10))
  }

  def createOnesRaster:Raster = {
    val arr = (for(i <- 1 to 100) yield 1).toArray
    Raster(arr, RasterExtent(Extent(0,0,10,10),1,1,10,10))
  }

  def checkSet(set:IntCellSet,cursor:IntCursor,m:Movement,center:(Int,Int),expected:Seq[Int]) = {
    center match { 
      case (x,y) =>
        val s = Set[Int]()
        val cb = new IntFocalValueCB { def act(v:Int) = { s += v } }
        cursor.centerOn(x,y)
        cursor.move(m)
        set.foreach(cb)
        s.toSeq.sorted should equal (expected)
      case _ => throw new Exception()
    }
  }

  def checkAdded(cursor:IntCursor,m:Movement,center:(Int,Int),expected:Seq[Int]) =
    checkSet(cursor.addedCells,cursor,m,center,expected)
  
  def checkRemoved(cursor:IntCursor,m:Movement,center:(Int,Int),expected:Seq[Int]) =
    checkSet(cursor.removedCells,cursor,m,center,expected)

  describe("Cursor") {
    it("should get all values for middle cursor and no mask") {
      val r = createRaster
      val cursor = new IntCursor(r,1)
      cursor.centerOn(5,5)
      cursor.allCells.getAll.sorted should equal (Seq(45,46,47,55,56,57,65,66,67))
    }

    it("should get all values for corner cursors and no mask") {
      val r = createRaster
      val cursor = new IntCursor(r,1)
      cursor.centerOn(0,0)
      cursor.allCells.getAll.sorted should equal (Seq(1,2,11,12))
      cursor.centerOn(9,0)
      cursor.allCells.getAll.sorted should equal (Seq(9,10,19,20))
      cursor.centerOn(0,9)
      cursor.allCells.getAll.sorted should equal (Seq(81,82,91,92))
      cursor.centerOn(9,9)
      cursor.allCells.getAll.sorted should equal (Seq(89,90,99,100))
     }

    it("should get all values for edge cursors and no mask") {
      val r = createRaster
      val cursor = new IntCursor(r,1)
      cursor.centerOn(5,0)
      cursor.allCells.getAll.sorted should equal (Seq(5,6,7,15,16,17))
      cursor.centerOn(0,5)
      cursor.allCells.getAll.sorted should equal (Seq(41,42,51,52,61,62))
      cursor.centerOn(9,5)
      cursor.allCells.getAll.sorted should equal (Seq(49,50,59,60,69,70))
      cursor.centerOn(5,9)
      cursor.allCells.getAll.sorted should equal (Seq(85,86,87,95,96,97))
     }

    it("should fold left on corners with no mask") {
      val r = createOnesRaster
      val cursor = new IntCursor(r,1)
      cursor.centerOn(0,0)
      cursor.allCells.foldLeft(0)(foldLeftCB) should be (4)
      cursor.centerOn(9,0)
      cursor.allCells.foldLeft(0)(foldLeftCB) should be (4)
      cursor.centerOn(0,9)
      cursor.allCells.foldLeft(0)(foldLeftCB) should be (4)
      cursor.centerOn(9,9)
      cursor.allCells.foldLeft(0)(foldLeftCB) should be (4)
    }

    it("should fold left on edges and no mask") {
      val r = createOnesRaster
      val cursor = new IntCursor(r,1)
      cursor.centerOn(5,0)
      cursor.allCells.foldLeft(0)(foldLeftCB) should be (6)
      cursor.centerOn(0,5)
      cursor.allCells.foldLeft(0)(foldLeftCB) should be (6)
      cursor.centerOn(9,5)
      cursor.allCells.foldLeft(0)(foldLeftCB) should be (6)
      cursor.centerOn(5,9)
      cursor.allCells.foldLeft(0)(foldLeftCB) should be (6)
    }
    
    it("should fold left for middle cursor and no mask") {
      val r = createOnesRaster
      val cursor = new IntCursor(r,1)
      cursor.centerOn(5,5)
      cursor.allCells.foldLeft(0)(foldLeftCB) should be (9)
    }

    it("should be able to mask triangle and foldLeft with sum") {
      val r = createRaster
      val cursor = new IntCursor(r,1)
      cursor.setMask { (x,y) => x+y > 2 }
      cursor.centerOn(2,2)
      // 12+13+14+22+23+32
      cursor.allCells.foldLeft(0)(foldLeftCB) should be (116)
    }

    it("should be able to mask circle and foldLeft with min") {
      val r = createRaster
      val n = Circle(3)
      val cursor = Cursor.getInt(r,n)
      cursor.centerOn(4,4)
      cursor.allCells.foldLeft(Int.MaxValue)(foldLeftMinCB) should be (15)
    }
    
    it("should be able to mask circle and foldLeft with min for edge case") {
      val r = createRaster
      val n = Circle(3)
      val cursor = Cursor.getInt(r,n)
      cursor.centerOn(9,9)
      cursor.allCells.foldLeft(Int.MaxValue)(foldLeftMinCB) should be (70)
    }

    it("should mask edge cases correctly") {
      val r = createRaster
      val cursor = TestCursor.fromString(r, """
                X 0 X
                0 X 0
                X 0 X
                                         """)
      // Mask cursor on left edge
      cursor.centerOn(0,2)
      cursor.allCells.getAll.sorted should equal (Seq(11,22,31))

      // Mask cursor on right edge
      cursor.centerOn(9,8)
      cursor.allCells.getAll.sorted should equal (Seq(80,89,100))

      // Mask cursor on top edge
      cursor.centerOn(4,0)
      cursor.allCells.getAll.sorted should equal (Seq(4,6,15))

      // Mask cursor on bottom edge
      cursor.centerOn(4,9)
      cursor.allCells.getAll.sorted should equal (Seq(85,94,96))
    }

    it("should mask corner cases correctly") {
      val r = createRaster
      val cursor = TestCursor.fromString(r, """
                  0 0 X X X
                  0 0 0 X X
                  X 0 0 0 X  
                  X X 0 0 0
                  X X X 0 0
                                           """)
      // Mask cursor at top left corner
      cursor.centerOn(0,0)
      cursor.allCells.getAll.sorted should equal (Seq(1,2,11,12,13,22,23))

      // Mask cursor at top right corner
      cursor.centerOn(9,0)
      cursor.allCells.getAll.sorted should equal (Seq(9,10,20))

      // Mask cursor at bottom right corner
      cursor.centerOn(9,9)
      cursor.allCells.getAll.sorted should equal (Seq(78,79,88,89,90,99,100))

      // Mask cursor at bottom left corner
      cursor.centerOn(0,9)
      cursor.allCells.getAll.sorted should equal (Seq(81,91,92))
    }

    it("should give correct x and y values through a foreach") {
      val r = createRaster
      val cursor = TestCursor.fromString(r,"""
                         X0X
                         000
                         X0X
                                         """)
      val s = Set[(Int,Int)]()
      val cb = new IntFocalCellCB { def act(x:Int,y:Int,v:Int) = { s += ((x,y)) } }
      // Middle 
      cursor.centerOn(4,4)
      cursor.allCells.foreach(cb)
      s should equal (Set((4,4),(3,4),(4,3),(5,4),(4,5)))

      // Added
      cursor.move(Movement.Up)
      s.clear()
      cursor.addedCells.foreach(cb)
      s should equal (Set((4,2),(3,3),(5,3)))
      // Removed
      cursor.centerOn(4,4)
      cursor.move(Movement.Right)
      s.clear()
      cursor.removedCells.foreach(cb)
      s should equal (Set((3,4),(4,3),(4,5)))
    }

    /**                     Tracking Movement                         */

    // Moves up

    it("should calculate added values correctly for a move up and no mask") {
      val r = createRaster
      val cursor = new IntCursor(r,1)
      val check = Function.uncurried((checkAdded _).curried(cursor)(Movement.Up))

      check((4,5), Seq(34,35,36))

      // edge cases
      check((0,5), Seq(31,32))    // left edge
      check((4,1), Seq())         // top edge
      check((9,5), Seq(39,40))    // right edge
      check((4,9), Seq(74,75,76)) // bottom edge
      
      // corner cases
      check((0,1), Seq())         // top left corner
      check((9,1), Seq())         // top right corner
      check((0,9), Seq(71,72))    // bottom left corner
      check((9,9), Seq(79,80))    // bottom right corner
    }

    it("should calculate removed values correctly for a move up and no mask") {
      val r = createRaster
      val cursor = new IntCursor(r,1)
      val check = Function.uncurried((checkRemoved _).curried(cursor)(Movement.Up))

      check((4,5), Seq(64,65,66))

      // edge cases
      check((0,5), Seq(61,62))    // left edge
      check((4,1), Seq(24,25,26)) // top edge
      check((9,5), Seq(69,70))    // right edge
      check((4,9), Seq())         // bottom edge
      
      // corner cases
      check((0,1), Seq(21,22))    // top left corner
      check((9,1), Seq(29,30))    // top right corner
      check((0,9), Seq())         // bottom left corner
      check((9,9), Seq())         // bottom right corner
    }

    it("should calculate added values correctly for a move up and a mask") {
      val r = createRaster
      val cursor = TestCursor.fromString(r, """
                 X X 0 X X
                 0 X 0 X 0              \ /
                 0 0 X 0 0               x (doesn't) mark(s) the spot!
                 0 X 0 X 0              / \
                 X X 0 X X
                                         """)
      val check = Function.uncurried((checkAdded _).curried(cursor)(Movement.Up))

      check((4,5), Seq(25,33,37,44,46,55))

      // edge cases
      check((0,5), Seq(21,33,42,51))       // left edge
      check((4,1), Seq(4,6,15))            // top edge
      check((9,5), Seq(30,38,49,60))       // right edge
      check((4,9), Seq(65,73,77,84,86,95)) // bottom edge
      
      // corner cases
      check((0,1), Seq(2,11))              // top left corner
      check((9,1), Seq(9,20))              // top right corner
      check((0,9), Seq(61,73,82,91))       // bottom left corner
      check((9,9), Seq(70,78,89,100))      // bottom right corner
    }

    it("should calculate removed values correctly for a move up and a mask") {
      val r = createRaster
      val cursor = TestCursor.fromString(r, """
                 0 0 X 0 0
                 0 X 0 X 0           
                 0 0 X 0 0         8
                 0 X 0 X 0         
                 0 0 X 0 0
                                         """)
      val check = Function.uncurried((checkRemoved _).curried(cursor)(Movement.Up))

      check((4,5), Seq(34,36,45,54,56,65,73,74,76,77))

      // edge cases
      check((0,5), Seq(32,41,52,61,72,73))       // left edge
      check((4,1), Seq(5,14,16,25,33,34,36,37))  // top edge
      check((9,5), Seq(39,50,59,70,78,79))       // right edge
      check((4,9), Seq(74,76,85,94,96))          // bottom edge
      
      // corner cases
      check((0,1), Seq(1,12,21,32,33))           // top left corner
      check((9,1), Seq(10,19,30,38,39))          // top right corner
      check((0,9), Seq(72,81,92))                // bottom left corner
      check((9,9), Seq(79,90,99))                // bottom right corner      
    }

    // Moves down

    it("should calculate added values correctly for a move down and no mask") {
      val r = createRaster
      val cursor = new IntCursor(r,1)
      val check = Function.uncurried((checkAdded _).curried(cursor)(Movement.Down))

      check((4,5), Seq(74,75,76))

      // edge cases
      check((0,5), Seq(71,72))    // left edge
      check((4,0), Seq(24,25,26)) // top edge
      check((9,5), Seq(79,80))    // right edge
      check((4,8), Seq())         // bottom edge
      
      // corner cases
      check((0,0), Seq(21,22))    // top left corner
      check((9,0), Seq(29,30))    // top right corner
      check((0,8), Seq())         // bottom left corner
      check((9,8), Seq())         // bottom right corner
    }

    it("should calculate removed values correctly for a move down and no mask") {
      val r = createRaster
      val cursor = new IntCursor(r,1)
      val check = Function.uncurried((checkRemoved _).curried(cursor)(Movement.Down))

      check((4,5), Seq(44,45,46))

      // edge cases
      check((0,5), Seq(41,42))    // left edge
      check((4,0), Seq())         // top edge
      check((9,5), Seq(49,50))    // right edge
      check((4,8), Seq(74,75,76)) // bottom edge
      
      // corner cases
      check((0,0), Seq())         // top left corner
      check((9,0), Seq())         // top right corner
      check((0,8), Seq(71,72))    // bottom left corner
      check((9,8), Seq(79,80))    // bottom right corner
    }

    it("should calculate added values correctly for a move down and a mask") {
      val r = createRaster
      val cursor = TestCursor.fromString(r, """
                 X X 0 X X
                 X 0 0 0 X
                 0 0 0 0 0         (╯°□°）╯︵ ┻━┻
                 X 0 0 0 X
                 X X 0 X X
                                         """)
      val check = Function.uncurried((checkAdded _).curried(cursor)(Movement.Down))

      check((4,5), Seq(63,67,74,76,85))

      // edge cases
      check((0,5), Seq(63,72,81))         // left edge
      check((4,0), Seq(13,17,24,26,35))   // top edge
      check((9,5), Seq(68,79,90))         // right edge
      check((4,8), Seq(93,97))            // bottom edge
      
      // corner cases
      check((0,0), Seq(13,22,31))         // top left corner
      check((9,0), Seq(18,29,40))         // top right corner
      check((0,8), Seq(93))               // bottom left corner
      check((9,8), Seq(98))               // bottom right corner
    }

    it("should calculate removed values correctly for a move down and a mask") {
      val r = createRaster
      val cursor = TestCursor.fromString(r, """
                 X X 0 X X
                 X 0 X 0 X
                 0 X 0 X 0         
                 X 0 X 0 X
                 X X 0 X X
                                         """)
      val check = Function.uncurried((checkRemoved _).curried(cursor)(Movement.Down))

      check((4,5), Seq(35,44,46,53,55,57,64,66,75))

      // edge cases
      check((0,5), Seq(31,42,51,53,62,71))         // left edge
      check((4,0), Seq(3,5,7,14,16,25))            // top edge
      check((9,5), Seq(40,49,58,60,69,80))         // right edge
      check((4,8), Seq(65,74,76,83,85,87,94,96))   // bottom edge
      
      // corner cases
      check((0,0), Seq(1,3,12,21))                 // top left corner
      check((9,0), Seq(8,10,19,30))                // top right corner
      check((0,8), Seq(61,72,81,83,92))            // bottom left corner
      check((9,8), Seq(70,79,88,90,99))            // bottom right corner
    }


    // Moves left

    it("should calculate added values correctly for a move left and no mask") {
      val r = createRaster
      val cursor = new IntCursor(r,1)
      val check = Function.uncurried((checkAdded _).curried(cursor)(Movement.Left))

      check((4,5), Seq(43,53,63))

      // edge cases
      check((1,5), Seq())         // left edge
      check((4,0), Seq(3,13))     // top edge
      check((9,5), Seq(48,58,68)) // right edge
      check((4,9), Seq(83,93))    // bottom edge
      
      // corner cases
      check((1,0), Seq())         // top left corner
      check((9,0), Seq(8,18))     // top right corner
      check((1,9), Seq())         // bottom left corner
      check((9,9), Seq(88,98))    // bottom right corner
    }

    it("should calculate removed values correctly for a move left and no mask") {
      val r = createRaster
      val cursor = new IntCursor(r,1)
      val check = Function.uncurried((checkRemoved _).curried(cursor)(Movement.Left))

      check((4,5), Seq(46,56,66))

      // edge cases
      check((1,5), Seq(43,53,63)) // left edge
      check((4,0), Seq(6,16))     // top edge
      check((9,5), Seq())         // right edge
      check((4,9), Seq(86,96))    // bottom edge
      
      // corner cases
      check((1,0), Seq(3,13))     // top left corner
      check((9,0), Seq())         // top right corner
      check((1,9), Seq(83,93))    // bottom left corner
      check((9,9), Seq())         // bottom right corner
    }

    it("should calculate added values correctly for a move left and a mask") {
      val r = createRaster
      val cursor = TestCursor.fromString(r, """
                 0 X 0 X 0
                 X X X X X
                 0 X 0 X 0        #
                 X X X X X
                 0 X 0 X 0
                                         """)
      val check = Function.uncurried((checkAdded _).curried(cursor)(Movement.Left))

      check((4,5), Seq(32,34,36,52,54,56,72,74,76))

      // edge cases
      check((1,5), Seq(31,33,51,53,71,73))    // left edge
      check((4,0), Seq(2,4,6,22,24,26))       // top edge
      check((9,5), Seq(37,39,57,59,77,79))    // right edge
      check((4,9), Seq(72,74,76,92,94,96))    // bottom edge
      
      // corner cases
      check((1,0), Seq(1,3,21,23))            // top left corner
      check((9,0), Seq(7,9,27,29))            // top right corner
      check((1,9), Seq(71,73,91,93))          // bottom left corner
      check((9,9), Seq(77,79,97,99))          // bottom right corner
    }

    it("should calculate removed values correctly for a move left and a mask") {
      val r = createRaster
      val cursor = TestCursor.fromString(r, """
                 0 X X X 0
                 X 0 0 0 X
                 X 0 X 0 X        
                 X 0 0 0 X
                 0 X X X 0
                                         """)
      val check = Function.uncurried((checkRemoved _).curried(cursor)(Movement.Left))

      check((4,5), Seq(33,37,46,54,56,66,73,77))

      // edge cases
      check((1,5), Seq(34,43,51,53,63,74))    // left edge
      check((4,0), Seq(4,6,16,23,27))         // top edge
      check((9,5), Seq(38,59,78))             // right edge
      check((4,9), Seq(73,77,86,94,96))       // bottom edge
      
      // corner cases
      check((1,0), Seq(1,3,13,24))            // top left corner
      check((9,0), Seq(9,28))                 // top right corner
      check((1,9), Seq(74,83,91,93))          // bottom left corner
      check((9,9), Seq(78,99))                // bottom right corner
    }

    // Moves right

    it("should calculate added values correctly for a move right and no mask") {
      val r = createRaster
      val cursor = new IntCursor(r,1)
      val check = Function.uncurried((checkAdded _).curried(cursor)(Movement.Right))

      check((4,5), Seq(47,57,67))

      // edge cases
      check((0,5), Seq(43,53,63)) // left edge
      check((4,0), Seq(7,17))     // top edge
      check((8,5), Seq())         // right edge
      check((4,9), Seq(87,97))    // bottom edge
      
      // corner cases
      check((0,0), Seq(3,13))     // top left corner
      check((8,0), Seq())         // top right corner
      check((0,9), Seq(83,93))    // bottom left corner
      check((8,9), Seq())         // bottom right corner
    }

    it("should calculate removed values correctly for a move right and no mask") {
      val r = createRaster
      val cursor = new IntCursor(r,1)
      val check = Function.uncurried((checkRemoved _).curried(cursor)(Movement.Right))

      check((4,5), Seq(44,54,64))

      // edge cases
      check((0,5), Seq())          // left edge
      check((4,0), Seq(4,14))      // top edge
      check((8,5), Seq(48,58,68))  // right edge
      check((4,9), Seq(84,94))     // bottom edge
      
      // corner cases
      check((0,0), Seq())          // top left corner
      check((8,0), Seq(8,18))      // top right corner
      check((0,9), Seq())          // bottom left corner
      check((8,9), Seq(88,98))     // bottom right corner
    }

    it("should calculate added values correctly for a move right and a mask") {
      val r = createRaster
      val cursor = TestCursor.fromString(r, """
                 0 0 0 X 0
                 0 0 X 0 X
                 0 X X X 0        
                 X 0 X 0 0
                 0 X 0 0 0
                                         """)
      val check = Function.uncurried((checkAdded _).curried(cursor)(Movement.Right))

      check((4,5), Seq(36,38,45,47,54,58,65,68,74,78))

      // edge cases
      check((0,5), Seq(32,34,41,43,54,61,64,74))   // left edge
      check((4,0), Seq(4,8,15,18,24,28))           // top edge
      check((8,5), Seq(40,49,58,69,78))            // right edge
      check((4,9), Seq(76,78,85,87,94,98))         // bottom edge
      
      // corner cases
      check((0,0), Seq(4,11,14,24))                // top left corner
      check((8,0), Seq(8,19,28))                   // top right corner
      check((0,9), Seq(72,74,81,83,94))            // bottom left corner
      check((8,9), Seq(80,89,98))                  // bottom right corner
    }

    it("should calculate removed values correctly for a move right and a mask") {
      val r = createRaster
      val cursor = TestCursor.fromString(r, """
                 0 X 0 X 0
                 0 0 X 0 X
                 0 X X X 0        
                 X 0 X 0 0
                 0 X 0 X 0
                                         """)
      val check = Function.uncurried((checkRemoved _).curried(cursor)(Movement.Right))

      cursor.centerOn(4,5)
      println(cursor.asciiDraw)
      check((4,5), Seq(33,35,37,43,46,53,57,64,66,73,75,77))

      // edge cases
      check((0,5), Seq(31,33,42,53,62,71,73))       // left edge
      check((4,0), Seq(3,7,14,16,23,25,27))         // top edge
      check((8,5), Seq(37,39,47,50,57,68,70,77,79)) // right edge
      check((4,9), Seq(73,75,77,83,86,93,97))       // bottom edge
      
      // corner cases
      check((0,0), Seq(3,12,21,23))                 // top left corner
      check((8,0), Seq(7,18,20,27,29))              // top right corner
      check((0,9), Seq(71,73,82,93))                // bottom left corner
      check((8,9), Seq(77,79,87,90,97))             // bottom right corner
    }
  }
}
