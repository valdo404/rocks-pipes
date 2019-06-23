import RocksPipe.{KV, RockingDatabase}
import cats.effect.ConcurrentEffect
import com.typesafe.scalalogging.LazyLogging
import fs2.Pipe
import org.rocksdb.{FlushOptions, RocksDB}
import scodec.codecs.{double, uuid}

case class SimpleIngestionApp[F[_] : ConcurrentEffect](db: RockingDatabase)
  extends RocksPipeApp[F] with LazyLogging {

  import cats.effect._
  import cats.implicits._

  val F = implicitly[ConcurrentEffect[F]]
  private val generationSteps = 10000000
  private val chunking = 10000

  def useDatabase: F[ExitCode] = {

    val columnFamilyPipe: Pipe[F, KV, Unit] = RocksPipe.columnFamilyPipe(db._1)( db._2(
      RocksPipe.defaultFamily), concurrent = 8)

    val dataGen: fs2.Stream[F, KV] = StreamGenerator.stream[F]
      .take(generationSteps)
      .chunkN(chunking)
      .flatMap { chunk =>
        fs2.Stream.chunk(chunk.map{ case (id, score ) => (
          uuid.encode(id).require.toByteArray,
          double.encode(score).require.toByteArray)})
      }

    dataGen.through(
      ingestToStore(columnFamilyPipe, RocksPipe.observingTap)
    ).compile.drain.as(ExitCode.Success) *>
      F.delay(db._1.flush(new FlushOptions().setWaitForFlush(true))) *>
      F.delay(db._1.compactRange()) *>
      scan(db._1, RocksPipe.observingTap)
  }


  private def scan(db: RocksDB,
                   observingTap: Pipe[F, KV, Unit]) = {
    RocksPipe.scan[F](db)
      .observe(observingTap)
      .onFinalizeCase {
        case ExitCase.Error(t) => F.delay(logger.error("Unexpected error", t))
        case ExitCase.Completed => F.delay(logger.info("Scanning is finished"))
        case _ => F.unit
      }
      .compile.drain
      .as(ExitCode.Success)
  }

  private def ingestToStore(store: Pipe[F, KV, Unit],
                            observingTap: Pipe[F, KV, Unit]): Pipe[F, KV, Unit] = {
    (_: fs2.Stream[F, (Array[Byte], Array[Byte])])
      .observe(observingTap)
      .observe(store)
      .onFinalizeCase {
        case ExitCase.Error(t) => F.delay(logger.error("Unexpected error", t))
        case ExitCase.Completed => F.delay(logger.info("Ingestion is finished"))
        case _ => F.unit
      }
      .drain
  }
}
