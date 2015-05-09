package hdfs


import java.net.URI

import latte.ui.hdfs.Types._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{PathFilter, FileSystem, FileUtil, Path}
import org.apache.hadoop.io.{BytesWritable, LongWritable, SequenceFile}

import scala.collection.mutable.ArrayBuffer


class Driver(host: String, port: String = "8020") {

  type RawLog = (Timestamp, RawMessage)
  type RawMessage = Array[Byte]
  type Timestamp = Long

  val uri = s"hdfs://$host:$port"
  val config = new Configuration()
  val hdfs: FileSystem = FileSystem.get(URI.create(uri), config)

  def listDir(path: String): Array[String] = {
     val fileStatus = hdfs.listStatus(new Path(s"$uri$path"), new PathFilter() {
       override def accept(path: Path): Boolean = {
         !path.toString.contains(".tmp")
       }
     })
     FileUtil.stat2Paths(fileStatus).map(a => a.toString)
  }



  def read(path: String): ArrayBuffer[RawLog] = {

    val result = ArrayBuffer[RawLog]()
    val reader = new SequenceFile.Reader(
      config,
      SequenceFile.Reader.file(new Path(path)))
    val x = new LongWritable()
    val y = new BytesWritable()

    def next(): ArrayBuffer[RawLog] =
      reader.next(x: LongWritable, y: BytesWritable) match {
        case true =>
          result += ((x.get, y.getBytes))
          next()
        case false => {
          result
        }
      }
    next()
  }
}

