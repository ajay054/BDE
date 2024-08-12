trait Vehicle {
  def startEngine(): Unit
  def stopEngine(): Unit
  def accelerate(speed: Double): Unit
}
class Car(val make: String, val model: String) extends Vehicle {
  override def startEngine(): Unit = {
    println(s"Starting the engine of $make $model")
  }

  override def stopEngine(): Unit = {
    println(s"Stopping the engine of $make $model")
  }

  override def accelerate(speed: Double): Unit = {
    println(s"Accelerating $make $model to $speed mph")
  }
}
class Bicycle(val brand: String) extends Vehicle {
  override def startEngine(): Unit = {
    println(s"Bicycles don't have engines, but let's pedal!")
  }

  override def stopEngine(): Unit = {
    println(s"Stopping the imaginary engine of $brand bicycle")
  }

  override def accelerate(speed: Double): Unit = {
    println(s"Pedaling faster on the $brand bicycle")
  }
}
val myCar = new Car("Toyota", "Camry")
myCar.startEngine()
myCar.accelerate(60.0)
myCar.stopEngine()

val myBicycle = new Bicycle("Mountain Bike")
myBicycle.startEngine()
myBicycle.accelerate(10.0)
myBicycle.stopEngine()
