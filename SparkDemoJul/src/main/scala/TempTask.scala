package org.itc.com

object TempTask extends App {
  import scala.io.Source
  import java.io.FileNotFoundException

  val filename = "Temp.txt"

  try {
    val lines = Source.fromFile(filename).getLines.toList
    // Rest of your processing logic here
  } catch {
    case e: FileNotFoundException =>
      println(s"Couldn't find the file '$filename'.")
  }

}

