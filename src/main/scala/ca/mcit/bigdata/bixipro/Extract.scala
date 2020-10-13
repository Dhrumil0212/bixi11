package ca.mcit.bigdata.bixipro
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

  trait  Main {

    val conf = new Configuration()

    conf.addResource(new Path("/home/bd-user/opt/hadoop/etc/cloudera/core-site.xml"))
    conf.addResource(new Path("/home/bd-user/opt/hadoop/etc/cloudera/hdfs-site.xml"))

//    val fs = FileSystem.get(conf)
//    if (fs.exists(new Path("/user/fall2019/dhrumil")))
//      if (fs.exists(new Path("/user/fall2019/dhrumil/sprint2")))
//        fs.delete(new Path("/user/fall2019/dhrumil/sprint2"), false)
//    fs.mkdirs(new Path("/user/fall2019/dhrumil/sprint2"))
//
//    val stationFile: Unit = fs.copyFromLocalFile(new Path("/home/bd-user/Downloads/station_information.json"), new Path("/user/fall2019/dhrumil/sprint2/station_information"))
//    val systemFile: Unit = fs.copyFromLocalFile(new Path("/home/bd-user/Downloads/system_information.json"), new Path("/user/fall2019/dhrumil/sprint2/system_information"))
  }
