package com.foo.scada

/**
 * Implementation of a Queue with restricted size.
 * @param limit Size limit of the queue. If more elements are pushed into the queue, the oldest will be removed first if size will be exceeded
 * @tparam T Type parameter for elements to be stored in queue
 */
class FiniteQueue[T](limit: Int) extends scala.collection.mutable.Queue[T] {
  override def enqueue(elems: T*) {
    elems.foreach { elem ⇒
      if (size >= limit) super.dequeue()
      this += elem
    }
  }
}