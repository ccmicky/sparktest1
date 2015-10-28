package com.test.Comm
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path, FileSystem, FSDataInputStream}
import scala.collection.mutable.ListBuffer

/**
 * Created by Administrator on 2015/10/22.
 */
class FileMethod {

  def GetHDFSFileContent(path: String, fileName: org.apache.hadoop.fs.Path): String = {
    val conf = new Configuration()
    val hdfs = FileSystem.get(URI.create(path), conf)

    val dis: FSDataInputStream = hdfs.open(fileName);

    try {
      var buf = ListBuffer[Byte]()

      var b = dis.read()
      while (b != -1) {
        buf.append(b.toByte)
        b = dis.read()
      }

      new String(buf.toArray)
    }
    finally {
      dis.close()
    }
  }


  def deleteHDFSFold(path:String,goalpath:String)=
  {
    val conf = new Configuration()
    val hdfs = FileSystem.get(URI.create(path),conf)
    val fs = hdfs.listStatus(new Path(path))
    val listPath = FileUtil.stat2Paths(fs)

    for( p <- listPath)
    {
      if ((path+goalpath).equals(p.toString))
      {
        hdfs.delete(p,true)
      }
    }
  }
}
