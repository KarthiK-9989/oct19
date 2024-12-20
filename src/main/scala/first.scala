// Stack implementation
class Stack[T] {
  private var elements: List[T] = List.empty

  def isEmpty: Boolean = elements.isEmpty

  def push(data: T): Unit = {
    elements = data :: elements
    print(elements)
  }


  def pop(): Option[T] = {
    if (isEmpty) {
      None
    } else {
      val top = elements.head
      //      println("top------------>",top)
      elements = elements.tail
      //      print("elements------------>",elements)
      Some(top)


    }
  }

  def peek(): Option[T] = {
    if (isEmpty) {
      None
    } else {
      Some(elements.head)
    }
  }
}

// Queue implementation
class Queue[T] {
  private var elements: List[T] = List.empty

  def isEmpty: Boolean = elements.isEmpty

  def enqueue(data: T): Unit = {
    elements = elements :+ data
//    println(elements)
  }


  def dequeue(): Option[T] = {
    if (isEmpty) {
      None
    } else {
      val front = elements.head
      println("front------------",front)
      elements = elements.tail
      println("elements------------",elements)
      Some(front)
    }
  }


//  def peek(): Option[T] = {
//    if (isEmpty) {
//      None
//    } else {
//      Some(elements.head)
//    }
//  }
}

object Main {
  def main(args: Array[String]): Unit = {
    // Example usage:
//        val stack = new Stack[Int]()
//        stack.push(10)
//        stack.push(20)
//        stack.push(30)
//        stack.push(40)
    //
    ////   println("----peeekkkkkkkkkkkkkkkk", stack.peek())



    //    println("Stack:")
    //    while (!stack.isEmpty) {
    //      println("--------stack.pop",stack.pop())
    //    }

    val queue = new Queue[String]()
    queue.enqueue("a")
    queue.enqueue("b")
    queue.enqueue("c")




      println("queue.dequeue().get------------------",queue.dequeue().get)

  }
}
