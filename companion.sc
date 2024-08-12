class Person(val name: String)

object Person {
  def create(name: String): Person = new Person(name)
}

val p = Person.create("Alice")
println(s"Hello, ${p.name}!")
