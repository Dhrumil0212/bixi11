package ca.mcit.bigdata.bixipro
import java.sql.{Connection, DriverManager}
import io.circe.{Decoder, HCursor, Json, parser}
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import scala.io.Source

object Enricher extends Main with App {

  val outputDir = new Path("/user/fall2019/dhrumil/sprint2/")
  if (fs.exists(outputDir)) fs.delete(outputDir, true)
  fs.mkdirs(outputDir)


  val systemInformationInput = Source.fromFile("/home/bd-user/Downloads/system_information.json").mkString
  val systemCsv = fs.create(new Path("/user/fall2019/dhrumil/sprint2/system_information/system_information.csv"), true)
  parser.decode[SystemInformation](systemInformationInput) match {
    case Right(value) => systemCsv.writeBytes(SystemInformation.toCsv(value))
    case Left(ex) => println(s"Exception is - $ex")
  }

  val stationInformationInput = Source.fromFile("/home/bd-user/Downloads/station_information.json").getLines().mkString
  val stationCsv: FSDataOutputStream = fs.create(new Path("/user/fall2019/dhrumil/sprint2/station_information/station_information.csv"), true)
  implicit val listStations: Decoder[List[Json]] = (c: HCursor) => {
    c.downField("data").downField("stations").as[List[Json]]
  }
  parser.decode[List[Json]](stationInformationInput) match {
    case Right(value) => value.map(value => {
      parser.decode[StationInformation](value.toString) match {
        case Right(value) => stationCsv.writeBytes(StationInformation.toCsv(value))
        case Left(ex) => println(s"Exception is :-> $ex")
      }
    })
    case Left(ex) => println(s"Exception in decoding of List of Stations :-> $ex")
  }

  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)


  val connection: Connection = DriverManager.getConnection("jdbc:hive2://172.16.129.58:10000", "dhrumil", "1234")
  val stmt = connection.createStatement()

  stmt.execute("DROP TABLE IF EXISTS s19909_bixi_feed_dhrumil.system_information")
  stmt.execute("CREATE EXTERNAL TABLE s19909_bixi_feed_dhrumil.system_information ( " +
    "system_id STRING, language STRING, name STRING, short_name STRING, operator STRING, url STRING, purchase_url STRING, start_date STRING, phone_number STRING, email STRING, timezone STRING, license_url STRING " +
    ") " +
    "ROW FORMAT DELIMITED " +
    "FIELDS TERMINATED BY ',' " +
    "STORED AS TEXTFILE " +
    "LOCATION '/user/fall2019/dhrumil/sprint2/system_information' ")

  stmt.execute("DROP TABLE IF EXISTS s19909_bixi_feed_dhrumil.station_information")
  stmt.execute(
    """CREATE EXTERNAL TABLE s19909_bixi_feed_dhrumil.station_information ( station_id String, external_id STRING, name STRING, short_name STRING, lat FLOAT, lon FLOAT, rental_methods Array<STRING>, capacity INT, electric_bike_surcharge_waiver BOOLEAN, eightd_has_key_dispenser BOOLEAN, has_kiosk BOOLEAN )
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ","
      |COLLECTION ITEMS TERMINATED BY "&"
      |STORED AS TEXTFILE
      |LOCATION '/user/fall2019/dhrumil/sprint2/station_information'
      |""".stripMargin)


  stmt.execute("INSERT OVERWRITE DIRECTORY '/user/fall2019/dhrumil/sprint2/Enricher' " +
    "ROW FORMAT DELIMITED " +
    "FIELDS TERMINATED BY ',' " +
    "SELECT * from s19909_bixi_feed_dhrumil.station_information LEFT JOIN s19909_bixi_feed_dhrumil.system_information")

  stmt.execute("DROP TABLE IF EXISTS s19909_bixi_feed_dhrumil.enriched_station_info")
  stmt.execute("CREATE external TABLE s19909_bixi_feed_dhrumil.enriched_station_info ( " +
    "system_Id STRING, " +
    "language STRING, " +
    "url STRING, " +
    "system_startDate DATE, " +
    "timezone STRING, " +
    "station_Id INT, " +
    "station_name STRING, " +
    "short_name INT, " +
    "capacity Int, " +
    "latitude FLOAT, " +
    "longitude FLOAT, " +
    "surcharge_waiver BOOLEAN, " +
    "key_dispenser BOOLEAN, " +
    "kiosk BOOLEAN, " +
    "extr_id string, " +
    "rental_method_a STRING, " +
    "rental_method_b STRING ) " +
    "ROW FORMAT DELIMITED " +
    "FIELDS TERMINATED BY ',' " +
    "STORED AS TEXTFILE " +
    "LOCATION '/user/fall2019/dhrumil/sprint2/Enricher'")

  stmt.close()
  connection.close()
}