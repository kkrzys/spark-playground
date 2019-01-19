package playground.utils

object RandomGenerator {

  private val rnd = new scala.util.Random

  def between(start: Int, end: Int): Int = {
    start + rnd.nextInt((end - start) + 1)
  }
}
