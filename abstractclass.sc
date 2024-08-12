abstract class Pet(name: String) {
    def speak(): Unit
}

class Dog(name: String) extends Pet(name) {
    override def speak(): Unit = println("Woof!")
}

val myDog = new Dog("Buddy")
myDog.speak() // Output: Woof!
