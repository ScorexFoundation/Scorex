package scorex.testkit.utils

import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._
import org.scalacheck.Gen

import scala.util.Try

trait FileUtils {

  protected val randomPrefixLength = 10

  val basePath: Path = java.nio.file.Files.createTempDirectory(s"scorex-${System.nanoTime()}")

  private def createTempDirForPrefix(prefix: String): java.io.File = {
    val path = java.nio.file.Files.createTempDirectory(basePath, prefix)
    sys.addShutdownHook {
      remove(path)
    }
    val file = path.toFile
    file.deleteOnExit()
    file
  }

  def createTempDir: java.io.File = {
    val rndString = scala.util.Random.alphanumeric.take(randomPrefixLength).mkString
    createTempDirForPrefix(rndString)
  }

  def tempDirGen: Gen[java.io.File] = Gen.listOfN(randomPrefixLength, Gen.alphaNumChar).map { p =>
    val prefix = p.mkString("")
    createTempDirForPrefix(prefix)
  }

  /**
    * Recursively remove all the files and directories in `root`
    */
  def remove(root: Path): Unit = {
    Files.walk(root).iterator().asScala.toSeq.reverse.foreach(path => Try(Files.delete(path)))
  }

}
