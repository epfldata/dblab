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
  def runOnData(runner: String => Unit): Unit = {
    getDataFolder() match {
      case Some(datapath) =>
        runner(datapath)
      case None => getEnvironmentVariable() match {
        case Some(path) => println(s"Tests could not run because $path is not a valid directory.")
        case None       => println("Tests could not run because the environment variable `LEGO_DATA_FOLDER` does not exist.")
      }
    }
  }
}
