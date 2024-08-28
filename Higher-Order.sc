// Higher-order function
def doubleAndPrint(a: Int, f: Int => Int): Unit = {
  val result = f(a)
  println(s"The value is now $result")
}

// Usage
doubleAndPrint(5, a => a * 2) // Prints: "The value is now 10"
