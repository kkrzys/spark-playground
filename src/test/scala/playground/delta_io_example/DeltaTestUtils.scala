package playground.delta_io_example

import java.io.File

import org.apache.commons.io.FileUtils

trait DeltaTestUtils {
  def withTempDir(dir: File => Unit): Unit = {
    val tmpDir = new File("delta_tables/tmp_table")
    try {
      dir(tmpDir)
    } finally {
      FileUtils.deleteDirectory(tmpDir)
    }
  }
}
