class Rational (n: Int, d: Int) {
  def this(n: Int) = this(n, 1)
  require(d != 0)

  private val g = gcd(n.abs, d.abs)
  val numer : Int = n / g
  val denom : Int = d / g

  override def toString: String = s"$numer/$denom"
  def + (that : Rational) : Rational = {
    new Rational(n = numer * that.denom + that.numer * denom,
      d = denom * that.denom
    )
  }
  def * (that : Rational) : Rational = {
    new Rational(numer * that.numer, denom * that.denom)
  }

  def add(that: Rational) : Rational = {
    new Rational(n = numer * that.denom + that.numer * denom,
      d = denom * that.denom
    )
  }

  private def gcd(a: Int, b: Int): Int = {
    if( b == 0 ) a else gcd(b, a % b)
  }

}
