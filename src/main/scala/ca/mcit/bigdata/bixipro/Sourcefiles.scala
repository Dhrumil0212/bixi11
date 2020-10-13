package ca.mcit.bigdata.bixipro
import java.io.{BufferedWriter, File, FileWriter}

import scala.io.Source
object Sourcefiles {
  def main(args: Array[String]): Unit = {

    val si = Source.fromURL("https://api-core.bixi.com/gbfs/en/station_information.json")
    val s = si.mkString


    siwriteFile("/home/bd-user/Downloads/station_information.json", s)

    val sys = Source.fromURL("https://api-core.bixi.com/gbfs/en/system_information.json")
    val sy = sys.mkString



    sywriteFile("/home/bd-user/Downloads/system_information.json", sy)

    println("Source files auto downloaded into LFS")
  }
  def siwriteFile(filename: String, s: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(s)
    bw.close()
  }
  def sywriteFile(filename: String, sy: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(sy)
    bw.close()
  }
}



