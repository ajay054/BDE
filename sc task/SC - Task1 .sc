////val input = List(
////  ("s1", "2016-01-01", 20.5),
////  ("s2", "2016-01-01", 30.1),
////  ("s1", "2016-01-02", 60.2),
////  ("s2", "2016-01-02", 20.4),
////  ("s1", "2016-01-03", 55.5),
////  ("s2", "2016-01-03", 52.5)
////)
//
//// Filter out invalid records (e.g., temperature is 9999)
//val validRecords = input.filter { case (_, _, temp) => temp != 9999 }
//
//// Find the highest temperature across all sensors
//val highestTemp = validRecords.map(_._3).max
//
//println(s"Highest temperature: $highestTemp")

import scala.io.Source
import java.io.FileNotFoundException

val filename = "temp.txt"

try {
  val lines = Source.fromFile(filename).getLines.toList
  // Rest of your processing logic here
} catch {
  case e: FileNotFoundException =>
    println(s"Couldn't find the file '$filename'.")
}

