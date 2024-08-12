// Closure example
def addN(fixedNumber: Int): Int => Int = {
  (value: Int) => value + fixedNumber
}

// Usage
val addFive = addN(5)
println(addFive(10)) // Prints: 15 (10 + 5)
