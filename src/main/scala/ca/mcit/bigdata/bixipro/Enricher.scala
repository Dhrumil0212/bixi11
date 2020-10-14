
import java.sql.{Connection, DriverManager}

import io.circe.Decoder.Result
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
    case Left(ex) => println(s"Exception is - ${ex}")
  }

  val stationInformationInput = Source.fromFile("/home/bd-user/Downloads/station_information.json").getLines().mkString
  val stationCsv: FSDataOutputStream = fs.create(new Path("/user/fall2019/dhrumil/sprint2/station_information/station_information.csv"), true)
  implicit val listStations = new Decoder[List[Json]] {
    override def apply(c: HCursor): Result[List[Json]] = {
      c.downField("data").downField("stations").as[List[Json]]
    }
  }
  parser.decode[List[Json]](stationInformationInput) match {
    case Right(value) => value.map(value => {
      parser.decode[StationInformation](value.toString) match {
        case Right(value) => stationCsv.writeBytes(StationInformation.toCsv(value))
        case Left(ex) => println(s"Exception is :-> ${ex}")
      }
    })
    case Left(ex) => println(s"Exception in decoding of List of Stations :-> ${ex}")
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


  stmt.execute("DROP TABLE IF EXISTS s19909_bixi_feed_dhrumil.enriched_station_info")
  stmt.execute("CREATE TABLE s19909_bixi_feed_dhrumil.enriched_station_info " +
    "( system_id STRING, language STRING, name STRING, short_name STRING, operator STRING, url STRING, purchase_url STRING, start_date STRING, phone_number STRING, email STRING, timezone STRING, license_url STRING,station_id String, external_id STRING, station_name String, station_short_name String, lat FLOAT, lon FLOAT, rental_methods Array<STRING>, capacity INT, electric_bike_surcharge_waiver BOOLEAN, eightd_has_key_dispenser BOOLEAN, has_kiosk BOOLEAN )" +
    "STORED AS TEXTFILE " +
    "LOCATION '/user/fall2019/dhrumil/sprint2/enriched_station_info/' ")

  stmt.execute(
    """ INSERT OVERWRITE TABLE s19909_bixi_feed_dhrumil.enriched_station_info
      | SELECT sys.system_id, sys.language, sys.name, sys.short_name, sys.operator, sys.url, sys.purchase_url, sys.start_date, sys.phone_number, sys.email, sys.timezone, sys.license_url, st.station_id, st.external_id, st.name, st.short_name, st.lat, st.lon, st.rental_methods, st.capacity, st.electric_bike_surcharge_waiver, st.eightd_has_key_dispenser, st.has_kiosk
      | FROM s19909_bixi_feed_dhrumil.system_information sys JOIN s19909_bixi_feed_dhrumil.station_information st
      |""".stripMargin)

  stmt.close()
  connection.close()
}