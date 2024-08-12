package org.itc.com


object Main {
  def sum(n1:Int, n2:Float) :Float = {
    return n1+n2

  }
var rate = 80
  def USDtoINR(n1: Int) :Float = {
    return n1* rate

  }
  def main(args: Array[String]): Unit = {
    println("Hello world!")
    println(sum(n1 = 10, n2 = 10.20f))

    //define a variable
    val pi:Float = 3.14f
    var age:Int = 20
    //Conditional statements
    if(age>60) {
      println("senior citizen")
    }else if(age>18)
      println("major")
    else {
      println("minor")
      //for loop
      for(i<- 0 to 10)
        println(i)
    }
  }
}
