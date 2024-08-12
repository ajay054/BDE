// Singleton object
object Logger {
  def log(message: String): Unit = {
    println(s"LOG: $message")
  }
}

// Usage
Logger.log("Hello, world!") // Prints: LOG: Hello, world!
