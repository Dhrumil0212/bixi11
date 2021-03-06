package ca.mcit.bigdata.bixipro
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait Main {

  val conf=new Configuration()

  conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))

  val fs: FileSystem = FileSystem.get(conf)

}
