package ch.epfl.data
package dblab.legobase
package storagemanager

object TPCHData {
  val DATAPATH = System.getenv("LEGO_DATA_FOLDER")

  def getEnvironmentVariable(): Option[String] = {
    Option(DATAPATH)
  }
  def getDataFolder(): Option[String] = {
    getEnvironmentVariable().flatMap(path => {
      val folder = new java.io.File(path)
      if (folder.isDirectory) {
        Some(s"$path/")
      } else {
        None
      }
    })
  }
}
