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

package geotrellis.raster.io

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode._
import scala.math.min

object Filesystem {
  val DEFAULT_BUFFER_READ_SIZE = 262144

  def slurp(path: String): Array[Byte] =
    slurp(new File(path))

  def slurp(path: String, bs: Int): Array[Byte] =
    slurp(new File(path), bs)

  def slurp(file: File): Array[Byte] =
    slurp(file, DEFAULT_BUFFER_READ_SIZE)

  def slurp(file: File, bs: Int): Array[Byte] = {
    val fis = new FileInputStream(file)
    val size = file.length.toInt
    val channel = fis.getChannel
    val buffer = channel.map(READ_ONLY, 0, size)
    fis.close
    
    // read 256K at a time out of the buffer into our array
    var i = 0
    val data = Array.ofDim[Byte](size)
    while(buffer.hasRemaining()) {
      val n = min(buffer.remaining(), bs)
      buffer.get(data, i, n)
      i += n
    }

    data
  }

  def mapToByteArray(path: String, data: Array[Byte], startIndex: Int, size: Int): Unit = {
    val f = new File(path)
    val fis = new FileInputStream(f)
    val buffer = 
      try {
        val channel = fis.getChannel
        channel.map(READ_ONLY, startIndex, size)
      } finally {
        fis.close
      }
    buffer.get(data, startIndex, size)
  }

  /**
   * Return the path string with the final extension removed.
   */
  def basename(p: String) = p.lastIndexOf(".") match {
    case -1 => p
    case n => p.substring(0, n)
  }

  def split(p: String) = p.lastIndexOf(".") match {
    case -1 => (p, "")
    case n => (p.substring(0, n), p.substring(n + 1, p.length))
  }

  def join(parts: String*) = parts.mkString(File.separator)

  def readText(path: String): String = {
    val src = scala.io.Source.fromFile(path)
    try {
      src.mkString
    } finally {
      src.close()
    }
  }
}
