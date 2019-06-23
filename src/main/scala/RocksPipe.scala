import java.util

import cats.effect.{ConcurrentEffect, Effect, IO, Resource}
import com.typesafe.scalalogging.LazyLogging
import fs2.{Chunk, INothing, Pipe}
import org.rocksdb.util.SizeUnit
import org.rocksdb._
import scodec.bits.ByteVector

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/** @todo move to class
  * @todo monitoring
  */
object RocksPipe extends LazyLogging {
  import scala.collection.JavaConverters._
  import cats.implicits._

  import fs2.Stream
  type KV = (Array[Byte], Array[Byte])
  val chunking = 100000
  val defaultFamily = ByteVector(RocksDB.DEFAULT_COLUMN_FAMILY)

  type RockingDatabase = (RocksDB, Map[ByteVector, ColumnFamilyHandle])

  def makeDatabase[F[_] : ConcurrentEffect](options: DBOptions, cfDescriptors: List[ColumnFamilyDescriptor], dbPath: String): Resource[F, RockingDatabase] = {
    val F = implicitly[Effect[F]]

    Resource.make(F.delay(initializeDb(options, cfDescriptors, dbPath))){case (db, _) =>
          F.delay(db.flush(new FlushOptions().setWaitForFlush(true))) *> F.delay(db.close())}
  }

  def discoveredFamilies(columnFamilies: mutable.Buffer[ColumnFamilyHandle]): Map[ByteVector, ColumnFamilyHandle] = columnFamilies
    .map(handle => ByteVector(handle.getName) -> handle).toMap

  def observingTap[F[_] : ConcurrentEffect]: fs2.Stream[F, KV] => fs2.Stream[F, INothing] = {
    val F = implicitly[Effect[F]]

    (_: fs2.Stream[F, (Array[Byte], Array[Byte])])
      .zipWithIndex
      .prefetchN(chunking)
      .parEvalMapUnordered(2){ case t @ (_, index) =>
        (if (index > 1 && index % chunking == 0)
          F.pure(logger.info(s"Step ${index / chunking} done"))
        else
          F.unit).as(t)
      }
      .last
      .evalTap {
        case Some((_, sum)) => F.delay(logger.info(s"Data set has size ${sum + 1}"))
        case None => F.delay(logger.error(s"No element found"))
      }
      .drain
  }

  /**
    * Build a pipe in order to store to a column family
    *
    * @todo specify options + should use batching ?
    */
  def columnFamilyPipe[F[_] : ConcurrentEffect]
        (db: RocksDB)
        (column: ColumnFamilyHandle, concurrent: Int = 4): Pipe[F, KV, Unit] = {
    val F = implicitly[Effect[F]]
    stream: Stream[F, KV] =>
      stream.chunkN(chunking).parEvalMapUnordered(concurrent)((chunk: Chunk[KV]) => {
        val options = new WriteOptions().setDisableWAL(false).setNoSlowdown(false).setSync(false)
        val batch = new WriteBatch
        chunk.foreach{ case (key, value) => batch.put(column, key, value) }

        F.delay(db.write(
          options, batch))
      }).drain
  }

  /**
    * Build a pipe in order to store to a column family
    *
    * @todo specify options + ability to specify several columns
    */
  def scan[F[_] : ConcurrentEffect](db: RocksDB): Stream[F, KV] = {
    rocksIterator(db).flatMap(fromIt).prefetchN(chunking)
  }

  private def rocksIterator[F[_] : ConcurrentEffect]
    (db: RocksDB): Stream[F, RocksIterator] = {
    val F = implicitly[Effect[F]]
    val readOptions = new ReadOptions().setFillCache(true)
      .setReadaheadSize(2 * SizeUnit.MB)
      .setTailing(true)

    fs2.Stream
      .resource(Resource.make
      (F.delay(db.newIterator(readOptions)))
      (iterator => F.delay(iterator.close())))
  }

  private def initializeDb(options: DBOptions, cfDescriptors: List[ColumnFamilyDescriptor], dbPath: String): RockingDatabase = {
    val cfList = ListBuffer.empty[ColumnFamilyHandle].asJava

    RocksDB.loadLibrary()
    val db = RocksDB.open(options, dbPath, cfDescriptors.asJava, cfList)
    (db, discoveredFamilies(cfList.asScala))
  }

  /**
    * Build a scala stream from a rocks db iterator
    */
  private def fromIt[F[_] : ConcurrentEffect](it: RocksIterator): Stream[F, KV] = {
    it.seekToFirst()

    val kvs = new Iterator[KV] {
      override def hasNext: Boolean =
        it.isValid

      override def next(): KV = {
        val kv = (it.key(), it.value())
        it.next()
        kv
      }
    }

    Stream.fromIterator(kvs)
  }
}
