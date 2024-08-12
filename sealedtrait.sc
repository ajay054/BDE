// Define the TailWagger trait
trait TailWagger {
  def startTail(): Unit
  def stopTail(): Unit
}

// Create a class for a specific animal (e.g., Dog)
class Dog(val name: String) extends TailWagger {
  override def startTail(): Unit =
    println(s"$name's tail is wagging happily!")

  override def stopTail(): Unit =
    println(s"$name's tail stopped wagging.")
}

// Usage
val myDog = new Dog("Buddy")
myDog.startTail() // Output: Buddy's tail is wagging happily!
myDog.stopTail()  // Output: Buddy's tail stopped wagging.
