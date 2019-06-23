import java.util.UUID

import cats.effect.Effect
import org.scalacheck.Gen

object StreamGenerator {

  import fs2.Stream

  val itemGen: Gen[List[(UUID, Double)]] = Gen.listOfN(10000, for {
    id <- Gen.uuid
    score <- Gen.chooseNum(0d, 1d)
  } yield (id, score))

  def stream[F[_] : Effect]: Stream[F, (UUID, Double)] = {
    Stream.suspend(fromOption(itemGen.sample)).flatMap(
      Stream.emits(_)
    ).repeat
  }

  def fromOption[R](o: Option[R]): Stream[fs2.Pure, R] = {
    o match {
      case None => Stream.empty
      case Some(e) => Stream.emit(e)
    }
  }
}
