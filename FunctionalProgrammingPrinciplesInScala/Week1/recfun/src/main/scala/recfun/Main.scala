package recfun

object Main {
  def main(args: Array[String]) {
    println("Pascal's Triangle")
    for (row <- 0 to 10) {
      for (col <- 0 to row)
        print(pascal(col, row) + " ")
      println()
    }
  }

  /**
   * Exercise 1
   */
    def pascal(c: Int, r: Int): Int = {
      /*
      def fac(n: Int, acc: Int): Int = {
        if ((n == 1) || (n == 0)) acc
        else fac(n - 1, acc * n)
      }
      fac(r, 1) /
        (fac(c, 1) * fac(r - c, 1))
       */
      if (c < 0 || r < 0)
        0
      else if (c == 0 || c == r)
        1
      else {
        pascal(c - 1, r - 1) + pascal(c, r - 1)
      }
    }
  
  /**
   * Exercise 2
   */
    def balance(chars: List[Char]): Boolean = {
      def balanceInner(chars: List[Char], left: Int): Boolean = {
        if (chars.isEmpty) (left == 0)
        else if (chars.head == '(')
          balanceInner(chars.tail, left + 1)
        else if (chars.head == ')') {
          if (left >= 1) balanceInner(chars.tail, left - 1)
          else false
        } else {
          balanceInner(chars.tail, left)
        }
      }
      balanceInner(chars, 0)
    }
  
  /**
   * Exercise 3
   */
    def countChange(money: Int, coins: List[Int]): Int = {
      if (money == 0) 1
      else if (money > 0 && !coins.isEmpty) {
        countChange(money, coins.tail) + countChange(money - coins.head, coins)
      } else 0
    }
  }
