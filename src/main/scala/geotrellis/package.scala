import scala.annotation.tailrec

//import annotation.implicitNotFound

package object geotrellis {
  final val NODATA = Int.MinValue

  type Op[+A] = Operation[A]
  type DI = DummyImplicit

  type RasterData = geotrellis.raster.RasterData
  
  /**
   * Add simple syntax for creating an operation.
   *
   * Define a function after op function that returns:
   *
   * 1) A literal value, e.g.
   *   val plusOne = op { (i:Int) => i + 1 }
   *
   * 2) An operation to be executed:
   *   val localPlusOne = ( (r:Raster, i:Int) => local.Add(r,i + 1) )
   *
   * 3) Or a StepResult (which indicates success or failure)
   *   val plusOne = op { (i:Int) => Result(i + 1) }
   *
   */

  // Op1 methods for op //

  /**
   * Create an operation from a 1-arg function that returns StepOutput.
   *
   * For example:
   *
   * val plusOne = op { (i:Int) => Result(i + 1) }
   */
  def op[A, T](f: (A) => StepOutput[T])(implicit m: Manifest[T]): (Op[A]) => Op1[A, T] =
    (a: Op[A]) => new Op1(a)((a) => f(a))

  /**
   * Create an operation from a 1-arg function that returns an operation to be executed.
   *
   * For example:
   *
   * val localPlusOne = ( (r:Raster, i:Int) => local.Add(r,i + 1) )
   *
   */
  def op[A, T](f: (A) => Op[T])(implicit m: Manifest[T], n: DI): (Op[A]) => Op1[A, T] =
    (a: Op[A]) => new Op1(a)((a) => StepRequiresAsync(List(f(a)), (l) => Result(l.head.asInstanceOf[T])))

  /**
   * Create an operation from a 1-arg function that returns a literal value.
   *
   * For example:
   *
   * val plusOne = op { (i:Int) => i + 1 }
   */
  def op[A, T](f: (A) => T)(implicit m: Manifest[T], n: DI, o: DI): (Op[A]) => Op1[A, T] =
    (a: Op[A]) => new Op1(a)((a) => Result(f(a)))

  // Op2 methods for op() //

  /**
   * Create an operation from a 2-arg function that returns StepOutput.
   */
  def op[A, B, T](f: (A, B) => StepOutput[T])(implicit m: Manifest[T]): (Op[A], Op[B]) => Op2[A, B, T] =
    (a: Op[A], b: Op[B]) => new Op2(a, b)((a, b) => f(a, b))

  /**
   * Create an operation from a 2-arg function that returns an operation.
   */
  def op[A, B, T](f: (A, B) => Op[T])(implicit m: Manifest[T], n: DI): (Op[A], Op[B]) => Op2[A, B, T] =
    (a: Op[A], b: Op[B]) => new Op2(a, b)((a, b) => StepRequiresAsync(List(f(a, b)), (l) =>
      Result(l.head.asInstanceOf[T])))

  /**
   * Create an operation from a 2-arg function that returns a literal value.
   */
  def op[A, B, T](f: (A, B) => T)(implicit m: Manifest[T], n: DI, o: DI): (Op[A], Op[B]) => Op2[A, B, T] =
    (a: Op[A], b: Op[B]) => new Op2(a, b)((a, b) => Result(f(a, b)))

  // Op3 methods for op() //

  /**
   * Create an operation from a 3-arg function that returns StepOutput.
   */
  def op[A, B, C, T](f: (A, B, C) => StepOutput[T])(implicit m: Manifest[T]): (Op[A], Op[B], Op[C]) => Op3[A, B, C, T] =
    (a: Op[A], b: Op[B], c: Op[C]) => new Op3(a, b, c)((a, b, c) => f(a, b, c))

  /**
   * Create an operation from a 3-arg function that returns an operation.
   */
  def op[A, B, C, T](f: (A, B, C) => Op[T])(implicit m: Manifest[T], n: DI): (Op[A], Op[B], Op[C]) => Op3[A, B, C, T] =
    (a: Op[A], b: Op[B], c: Op[C]) =>
      new Op3(a, b, c)((a, b, c) => StepRequiresAsync(List(f(a, b, c)), (l) => Result(l.head.asInstanceOf[T])))

  /**
   * Create an operation from a 3-arg function that returns a literal value.
   */
  def op[A, B, C, T](f: (A, B, C) => T)(implicit m: Manifest[T], n: DI, o: DI): (Op[A], Op[B], Op[C]) => Op3[A, B, C, T] =
    (a: Op[A], b: Op[B], c: Op[C]) => new Op3(a, b, c)((a, b, c) => Result(f(a, b, c)))

  // Op4 methods for op() //

  /**
   * Create an operation from a 4-arg function that returns StepOutput.
   */
  def op[A, B, C, D, T](f: (A, B, C, D) => StepOutput[T])(implicit m: Manifest[T]): 
	  (Op[A], Op[B], Op[C], Op[D]) => Op4[A, B, C, D, T] =
    (a: Op[A], b: Op[B], c: Op[C], d: Op[D]) => new Op4(a, b, c, d)((a, b, c, d) => f(a, b, c, d))

  /**
   * Create an operation from a 4-arg function that returns an operation.
   */
  def op[A, B, C, D, T](f: (A, B, C, D) => Op[T])(implicit m: Manifest[T], n: DI): 
	  (Op[A], Op[B], Op[C], Op[D]) => Op4[A, B, C, D, T] =
    (a: Op[A], b: Op[B], c: Op[C], d: Op[D]) => new Op4(a, b, c, d)((a, b, c, d) => 
      StepRequiresAsync(List(f(a, b, c, d)), (l) => Result(l.head.asInstanceOf[T])))
  
  /**
   * Create an operation from a 4-arg function that returns a literal value.
   */
  def op[A, B, C, D, T](f: (A, B, C, D) => T)(implicit m: Manifest[T], n: DI, o: DI): 
	  (Op[A], Op[B], Op[C], Op[D]) => Op4[A, B, C, D, T] =
    (a: Op[A], b: Op[B], c: Op[C], d: Op[D]) => new Op4(a, b, c, d)((a, b, c, d) => 
      Result(f(a, b, c, d)))

  /**
   * Execute operation with temporary server instance.
   */
  def execute[T:Manifest](op:Operation[T]) = {
    val s = process.Server.empty("execute")
    val result = s.run(op)
    s.shutdown()
    result 
  }
}

package geotrellis {
  package object process {
    type Callback[T] = (List[Any]) => StepOutput[T]
    type Args = List[Any]

    def time() = System.currentTimeMillis
    def log(msg: String) = if (false) println(msg)
  }

  package object util {
    /**
     * This function uses an associative binary function "f" to combine the
     * elements of a List[A] into a Option[A].
     *
     * If the list is empty, None is returned.
     * If the list is non-empty, Some[A] will be returned.
     *
     * For example, List(1,2,3,4)(f) results in Some(f(f(3, 4), f(1, 2))).
     */
    @tailrec
    def reducePairwise[A](as: List[A])(f: (A, A) => A): Option[A] = as match {
      case Nil      => None
      case a :: Nil => Some(a)
      case as       => reducePairwise(pairwise(as, Nil)(f))(f)
    }

    /**
     * This function uses an associative binary function "f" to combine
     * elements of a List[A] pairwise into a shorter List[A].
     *
     * For instance, List(1,2,3,4,5) results in List(5, f(3, 4), f(1, 2)).
     */
    @tailrec
    def pairwise[A](as: List[A], sofar: List[A])(f: (A, A) => A): List[A] = {
      as match {
        case a1 :: a2 :: as => pairwise(as, f(a1, a2) :: sofar)(f)
        case a :: Nil       => a :: sofar
        case Nil            => sofar
      }
    }
  }
}
