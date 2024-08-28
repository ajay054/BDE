val input = List(
  ("s1", "2016-01-01", 20.5),
  ("s2", "2016-01-01", 30.1),
  ("s1", "2016-01-02", 60.2),
  ("s2", "2016-01-02", 20.4),
  ("s1", "2016-01-03", 55.5),
  ("s2", "2016-01-03", 52.5)
)
// Filter out records where the temperature is greater than 50
val validRecords = input.filter { case (_, _, temp) => temp > 50 }
// Group by sensor ID and count the valid records
val countBySensor = validRecords.groupBy(_._1).mapValues(_.size)
// Print the result
countBySensor.foreach { case (sensor, count) =>
  println(s"$count, $sensor")
}
