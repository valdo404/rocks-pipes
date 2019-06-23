import java.util

import cats.effect._
import com.typesafe.scalalogging.LazyLogging
import fs2.{Chunk, INothing, Pipe}
import org.rocksdb._
import org.rocksdb.util.SizeUnit

object Main extends IOApp with
  LazyLogging {
  import cats.effect._
  import org.rocksdb.{ColumnFamilyDescriptor, RocksDB}

  // @todo streaming join
  // @todo multi column
  // @todo load from config
  // @todo try hdfs storage
  // @todo create pipes with config
  // @todo shortcuts

  import org.rocksdb.Env
  import org.rocksdb.RocksMemEnv

  val env = new RocksMemEnv(Env.getDefault)
  val options: DBOptions = new DBOptions()
    //.setEnv(env)
    .setCreateIfMissing(true)
    .setCreateMissingColumnFamilies(true)
    .setAllowConcurrentMemtableWrite(true)
    .setAllowMmapReads(true)

  val columnFamilyOptions = new ColumnFamilyOptions()
    .setNumLevels(4)
    .setMaxBytesForLevelBase(512 * SizeUnit.MB)
    .setMaxBytesForLevelMultiplier(8)
    .optimizeUniversalStyleCompaction
    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
    .setCompressionOptions(new CompressionOptions().setEnabled(true))
    .setMemTableConfig(new SkipListMemTableConfig())
    .setWriteBufferSize(64 * SizeUnit.MB)

  val cfDescriptors = List(
    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
    new ColumnFamilyDescriptor("my-first-columnfamily".getBytes, columnFamilyOptions))

  val dbPath = "maBase"

  override def run(args: List[String]): IO[ExitCode] = {
    val stats = options.statistics()

    RocksPipe.makeDatabase[IO](options, cfDescriptors, dbPath)
      .use(SimpleIngestionApp[IO].tupled(_).useDatabase)
  }
}



