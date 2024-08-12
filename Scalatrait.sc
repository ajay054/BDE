// Scala trait
trait Pet {
  def speak(): Unit
}

// Class mixing in the trait
class Dog(val name: String) extends Pet {
  override def speak(): Unit = println(s"$name says Woof!")
}
