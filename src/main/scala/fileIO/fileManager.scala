package fileIO

import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._
import exception.MyLittleException

object fileManager {
  def getFilesinDir(path : String): Iterator[Path] = {
    val dirPath = Paths.get(path)

    if(Files.exists(dirPath) && Files.isDirectory(dirPath)){
      val fileStream = Files.list(dirPath).iterator().asScala

      val result = for(file <- fileStream) yield file

      result
    } else {
      throw new MyLittleException(s"there is no $dirPath or $dirPath is not a directory")
    }
  }
}
