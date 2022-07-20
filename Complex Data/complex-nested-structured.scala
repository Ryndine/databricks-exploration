// COMMAND ----------

// COMMAND ----------

import org.apache.spark.sql.types._ 
import org.apache.spark.sql.functions._

val jsonSchema = new StructType()
        .add("battery_level", LongType)
        .add("c02_level", LongType)
        .add("cca3",StringType)
        .add("cn", StringType)
        .add("device_id", LongType)
        .add("device_type", StringType)
        .add("signal", LongType)
        .add("ip", StringType)
        .add("temp", LongType)
        .add("timestamp", TimestampType)

// COMMAND ----------

case class DeviceData (id: Int, device: String)
val eventsDS = Seq (
 (0, """{"device_id": 0, "device_type": "sensor-ipad", "ip": "68.161.225.1", "cca3": "USA", "cn": "United States", "temp": 25, "signal": 23, "battery_level": 8, "c02_level": 917, "timestamp" :1475600496 }"""),
 (1, """{"device_id": 1, "device_type": "sensor-igauge", "ip": "213.161.254.1", "cca3": "NOR", "cn": "Norway", "temp": 30, "signal": 18, "battery_level": 6, "c02_level": 1413, "timestamp" :1475600498 }"""),
 (2, """{"device_id": 2, "device_type": "sensor-ipad", "ip": "88.36.5.1", "cca3": "ITA", "cn": "Italy", "temp": 18, "signal": 25, "battery_level": 5, "c02_level": 1372, "timestamp" :1475600500 }"""),
 (3, """{"device_id": 3, "device_type": "sensor-inest", "ip": "66.39.173.154", "cca3": "USA", "cn": "United States", "temp": 47, "signal": 12, "battery_level": 1, "c02_level": 1447, "timestamp" :1475600502 }"""),
(4, """{"device_id": 4, "device_type": "sensor-ipad", "ip": "203.82.41.9", "cca3": "PHL", "cn": "Philippines", "temp": 29, "signal": 11, "battery_level": 0, "c02_level": 983, "timestamp" :1475600504 }"""),
(5, """{"device_id": 5, "device_type": "sensor-istick", "ip": "204.116.105.67", "cca3": "USA", "cn": "United States", "temp": 50, "signal": 16, "battery_level": 8, "c02_level": 1574, "timestamp" :1475600506 }"""),
(6, """{"device_id": 6, "device_type": "sensor-ipad", "ip": "220.173.179.1", "cca3": "CHN", "cn": "China", "temp": 21, "signal": 18, "battery_level": 9, "c02_level": 1249, "timestamp" :1475600508 }"""),
(7, """{"device_id": 7, "device_type": "sensor-ipad", "ip": "118.23.68.227", "cca3": "JPN", "cn": "Japan", "temp": 27, "signal": 15, "battery_level": 0, "c02_level": 1531, "timestamp" :1475600512 }"""),
(8 ,""" {"device_id": 8, "device_type": "sensor-inest", "ip": "208.109.163.218", "cca3": "USA", "cn": "United States", "temp": 40, "signal": 16, "battery_level": 9, "c02_level": 1208, "timestamp" :1475600514 }"""),
(9,"""{"device_id": 9, "device_type": "sensor-ipad", "ip": "88.213.191.34", "cca3": "ITA", "cn": "Italy", "temp": 19, "signal": 11, "battery_level": 0, "c02_level": 1171, "timestamp" :1475600516 }"""),
(10,"""{"device_id": 10, "device_type": "sensor-igauge", "ip": "68.28.91.22", "cca3": "USA", "cn": "United States", "temp": 32, "signal": 26, "battery_level": 7, "c02_level": 886, "timestamp" :1475600518 }"""),
(11,"""{"device_id": 11, "device_type": "sensor-ipad", "ip": "59.144.114.250", "cca3": "IND", "cn": "India", "temp": 46, "signal": 25, "battery_level": 4, "c02_level": 863, "timestamp" :1475600520 }"""),
(12, """{"device_id": 12, "device_type": "sensor-igauge", "ip": "193.156.90.200", "cca3": "NOR", "cn": "Norway", "temp": 18, "signal": 26, "battery_level": 8, "c02_level": 1220, "timestamp" :1475600522 }"""),
(13, """{"device_id": 13, "device_type": "sensor-ipad", "ip": "67.185.72.1", "cca3": "USA", "cn": "United States", "temp": 34, "signal": 20, "battery_level": 8, "c02_level": 1504, "timestamp" :1475600524 }"""),
(14, """{"device_id": 14, "device_type": "sensor-inest", "ip": "68.85.85.106", "cca3": "USA", "cn": "United States", "temp": 39, "signal": 17, "battery_level": 8, "c02_level": 831, "timestamp" :1475600526 }"""),
(15, """{"device_id": 15, "device_type": "sensor-ipad", "ip": "161.188.212.254", "cca3": "USA", "cn": "United States", "temp": 27, "signal": 26, "battery_level": 5, "c02_level": 1378, "timestamp" :1475600528 }"""),
(16, """{"device_id": 16, "device_type": "sensor-igauge", "ip": "221.3.128.242", "cca3": "CHN", "cn": "China", "temp": 10, "signal": 24, "battery_level": 6, "c02_level": 1423, "timestamp" :1475600530 }"""),
(17, """{"device_id": 17, "device_type": "sensor-ipad", "ip": "64.124.180.215", "cca3": "USA", "cn": "United States", "temp": 38, "signal": 17, "battery_level": 9, "c02_level": 1304, "timestamp" :1475600532 }"""),
(18, """{"device_id": 18, "device_type": "sensor-igauge", "ip": "66.153.162.66", "cca3": "USA", "cn": "United States", "temp": 26, "signal": 10, "battery_level": 0, "c02_level": 902, "timestamp" :1475600534 }"""),
(19, """{"device_id": 19, "device_type": "sensor-ipad", "ip": "193.200.142.254", "cca3": "AUT", "cn": "Austria", "temp": 32, "signal": 27, "battery_level": 5, "c02_level": 1282, "timestamp" :1475600536 }""")).toDF("id", "device").as[DeviceData]

// COMMAND ----------

display(eventsDS)

// COMMAND ----------

eventsDS.printSchema

// COMMAND ----------

val eventsFromJSONDF = Seq (
 (0, """{"device_id": 0, "device_type": "sensor-ipad", "ip": "68.161.225.1", "cca3": "USA", "cn": "United States", "temp": 25, "signal": 23, "battery_level": 8, "c02_level": 917, "timestamp" :1475600496 }"""),
 (1, """{"device_id": 1, "device_type": "sensor-igauge", "ip": "213.161.254.1", "cca3": "NOR", "cn": "Norway", "temp": 30, "signal": 18, "battery_level": 6, "c02_level": 1413, "timestamp" :1475600498 }"""),
 (2, """{"device_id": 2, "device_type": "sensor-ipad", "ip": "88.36.5.1", "cca3": "ITA", "cn": "Italy", "temp": 18, "signal": 25, "battery_level": 5, "c02_level": 1372, "timestamp" :1475600500 }"""),
 (3, """{"device_id": 3, "device_type": "sensor-inest", "ip": "66.39.173.154", "cca3": "USA", "cn": "United States", "temp": 47, "signal": 12, "battery_level": 1, "c02_level": 1447, "timestamp" :1475600502 }"""),
(4, """{"device_id": 4, "device_type": "sensor-ipad", "ip": "203.82.41.9", "cca3": "PHL", "cn": "Philippines", "temp": 29, "signal": 11, "battery_level": 0, "c02_level": 983, "timestamp" :1475600504 }"""),
(5, """{"device_id": 5, "device_type": "sensor-istick", "ip": "204.116.105.67", "cca3": "USA", "cn": "United States", "temp": 50, "signal": 16, "battery_level": 8, "c02_level": 1574, "timestamp" :1475600506 }"""),
(6, """{"device_id": 6, "device_type": "sensor-ipad", "ip": "220.173.179.1", "cca3": "CHN", "cn": "China", "temp": 21, "signal": 18, "battery_level": 9, "c02_level": 1249, "timestamp" :1475600508 }"""),
(7, """{"device_id": 7, "device_type": "sensor-ipad", "ip": "118.23.68.227", "cca3": "JPN", "cn": "Japan", "temp": 27, "signal": 15, "battery_level": 0, "c02_level": 1531, "timestamp" :1475600512 }"""),
(8 ,""" {"device_id": 8, "device_type": "sensor-inest", "ip": "208.109.163.218", "cca3": "USA", "cn": "United States", "temp": 40, "signal": 16, "battery_level": 9, "c02_level": 1208, "timestamp" :1475600514 }"""),
(9,"""{"device_id": 9, "device_type": "sensor-ipad", "ip": "88.213.191.34", "cca3": "ITA", "cn": "Italy", "temp": 19, "signal": 11, "battery_level": 0, "c02_level": 1171, "timestamp" :1475600516 }""")).toDF("id", "json")

// COMMAND ----------

display(eventsFromJSONDF)

// COMMAND ----------

val jsDF = eventsFromJSONDF.select($"id", get_json_object($"json", "$.device_type").alias("device_type"),
                                          get_json_object($"json", "$.ip").alias("ip"),
                                         get_json_object($"json", "$.cca3").alias("cca3"))

// COMMAND ----------

display(jsDF)

// COMMAND ----------

val devicesDF = eventsDS.select(from_json($"device", jsonSchema) as "devices")
.select($"devices.*")
.filter($"devices.temp" > 10 and $"devices.signal" > 15)

// COMMAND ----------

display(devicesDF)

// COMMAND ----------

val devicesUSDF = devicesDF.select($"*").where($"cca3" === "USA").orderBy($"signal".desc, $"temp".desc)

// COMMAND ----------

display(devicesUSDF)

// COMMAND ----------

val stringJsonDF = eventsDS.select(to_json(struct($"*"))).toDF("devices")

// COMMAND ----------

display(stringJsonDF)

// COMMAND ----------

// stringJsonDF.write
//            .format("kafka")
//            .option("kafka.bootstrap.servers", "your_host_name:9092")
//            .option("topic", "iot-devices")
//            .save()

// COMMAND ----------

val stringsDF = eventsDS.selectExpr("CAST(id AS INT)", "CAST(device AS STRING)")

// COMMAND ----------

stringsDF.printSchema

// COMMAND ----------

display(stringsDF)

// COMMAND ----------

display(devicesDF.selectExpr("c02_level", "round(c02_level/temp) as ratio_c02_temperature").orderBy($"ratio_c02_temperature" desc))

// COMMAND ----------

devicesDF.createOrReplaceTempView("devicesDFT")

// COMMAND ----------

stringJsonDF
  .write
  .mode("overwrite")
  .format("parquet")
  .save("/tmp/iot")

// COMMAND ----------

val parquetDF = spark.read.parquet("/tmp/iot")

// COMMAND ----------

parquetDF.printSchema

// COMMAND ----------

display(parquetDF)

// COMMAND ----------

import org.apache.spark.sql.types._

val schema = new StructType()
  .add("dc_id", StringType)
  .add("source",
    MapType(
      StringType,
      new StructType()
      .add("description", StringType)
      .add("ip", StringType)
      .add("id", LongType)
      .add("temp", LongType)
      .add("c02_level", LongType)
      .add("geo", 
         new StructType()
          .add("lat", DoubleType)
          .add("long", DoubleType)
        )
      )
    )

// COMMAND ----------

val dataDS = Seq("""
{
"dc_id": "dc-101",
"source": {
    "sensor-igauge": {
      "id": 10,
      "ip": "68.28.91.22",
      "description": "Sensor attached to the container ceilings",
      "temp":35,
      "c02_level": 1475,
      "geo": {"lat":38.00, "long":97.00}                        
    },
    "sensor-ipad": {
      "id": 13,
      "ip": "67.185.72.1",
      "description": "Sensor ipad attached to carbon cylinders",
      "temp": 34,
      "c02_level": 1370,
      "geo": {"lat":47.41, "long":-122.00}
    },
    "sensor-inest": {
      "id": 8,
      "ip": "208.109.163.218",
      "description": "Sensor attached to the factory ceilings",
      "temp": 40,
      "c02_level": 1346,
      "geo": {"lat":33.61, "long":-111.89}
    },
    "sensor-istick": {
      "id": 5,
      "ip": "204.116.105.67",
      "description": "Sensor embedded in exhaust pipes in the ceilings",
      "temp": 40,
      "c02_level": 1574,
      "geo": {"lat":35.93, "long":-85.46}
    }
  }
}""").toDS()
dataDS.count()

// COMMAND ----------

display(dataDS)

// COMMAND ----------

val df = spark
.read
.schema(schema)
.json(dataDS.rdd)

// COMMAND ----------

df.printSchema

// COMMAND ----------

display(df)

// COMMAND ----------

val explodedDF = df.select($"dc_id", explode($"source"))
display(explodedDF)

// COMMAND ----------

explodedDF.printSchema

// COMMAND ----------

case class DeviceAlert(dcId: String, deviceType:String, ip:String, deviceId:Long, temp:Long, c02_level: Long, lat: Double, lon: Double)
val notifydevicesDS = explodedDF.select( $"dc_id" as "dcId",
                        $"key" as "deviceType",
                        'value.getItem("ip") as 'ip,
                        'value.getItem("id") as 'deviceId,
                        'value.getItem("c02_level") as 'c02_level,
                        'value.getItem("temp") as 'temp,
                        'value.getItem("geo").getItem("lat") as 'lat, 
                        'value.getItem("geo").getItem("long") as 'lon)
                        .as[DeviceAlert]

// COMMAND ----------

notifydevicesDS.printSchema

// COMMAND ----------

display(notifydevicesDS)

// COMMAND ----------

object DeviceNOCAlerts {

  def sendTwilio(message: String): Unit = {
    //TODO: fill as necessary
    println("Twilio:" + message)
  }

  def sendSNMP(message: String): Unit = {
    //TODO: fill as necessary
    println("SNMP:" + message)
  }
  
  def sendKafka(message: String): Unit = {
    //TODO: fill as necessary
     println("KAFKA:" + message)
  }
}
def logAlerts(log: java.io.PrintStream = Console.out, deviceAlert: DeviceAlert, alert: String, notify: String ="twilio"): Unit = {
val message = "[***ALERT***: %s; data_center: %s, device_name: %s, temperature: %d; device_id: %d ; ip: %s ; c02: %d]" format(alert, deviceAlert.dcId, deviceAlert.deviceType,deviceAlert.temp, deviceAlert.deviceId, deviceAlert.ip, deviceAlert.c02_level)                                                                                                                                                                                                                                                            
  log.println(message)
  val notifyFunc = notify match {
      case "twilio" => DeviceNOCAlerts.sendTwilio _
      case "snmp" => DeviceNOCAlerts.sendSNMP _
      case "kafka" => DeviceNOCAlerts.sendKafka _
  }
  notifyFunc(message)
}

// COMMAND ----------


notifydevicesDS.foreach(d => logAlerts(Console.err, d, "ACTION NEED! HIGH TEPERATURE AND C02 LEVLES", "kafka"))

// COMMAND ----------

// val deviceAlertQuery = notifydevicesDS
//                       .selectExpr("CAST(dcId AS STRING) AS key", "to_json(struct(*)) AS value")
//                       .writeStream
//                       .format("kafka")
//                       .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
//                       .option("toipic", "device_alerts")
//                       .start()
                       

// COMMAND ----------

import org.apache.spark.sql.types._

val nestSchema2 = new StructType()
      .add("devices", 
        new StructType()
          .add("thermostats", MapType(StringType,
            new StructType()
              .add("device_id", StringType)
              .add("locale", StringType)
              .add("software_version", StringType)
              .add("structure_id", StringType)
              .add("where_name", StringType)
              .add("last_connection", StringType)
              .add("is_online", BooleanType)
              .add("can_cool", BooleanType)
              .add("can_heat", BooleanType)
              .add("is_using_emergency_heat", BooleanType)
              .add("has_fan", BooleanType)
              .add("fan_timer_active", BooleanType)
              .add("fan_timer_timeout", StringType)
              .add("temperature_scale", StringType)
              .add("target_temperature_f", DoubleType)
              .add("target_temperature_high_f", DoubleType)
              .add("target_temperature_low_f", DoubleType)
              .add("eco_temperature_high_f", DoubleType)
              .add("eco_temperature_low_f", DoubleType)
              .add("away_temperature_high_f", DoubleType)
              .add("away_temperature_low_f", DoubleType)
              .add("hvac_mode", StringType)
              .add("humidity", LongType)
              .add("hvac_state", StringType)
              .add("is_locked", StringType)
              .add("locked_temp_min_f", DoubleType)
              .add("locked_temp_max_f", DoubleType)))
           .add("smoke_co_alarms", MapType(StringType,
             new StructType()
             .add("device_id", StringType)
             .add("locale", StringType)
             .add("software_version", StringType)
             .add("structure_id", StringType)
             .add("where_name", StringType)
             .add("last_connection", StringType)
             .add("is_online", BooleanType)
             .add("battery_health", StringType)
             .add("co_alarm_state", StringType)
             .add("smoke_alarm_state", StringType)
             .add("is_manual_test_active", BooleanType)
             .add("last_manual_test_time", StringType)
             .add("ui_color_state", StringType)))
           .add("cameras", MapType(StringType, 
               new StructType()
                .add("device_id", StringType)
                .add("software_version", StringType)
                .add("structure_id", StringType)
                .add("where_name", StringType)
                .add("is_online", BooleanType)
                .add("is_streaming", BooleanType)
                .add("is_audio_input_enabled", BooleanType)
                .add("last_is_online_change", StringType)
                .add("is_video_history_enabled", BooleanType)
                .add("web_url", StringType)
                .add("app_url", StringType)
                .add("is_public_share_enabled", BooleanType)
                .add("activity_zones",
                  new StructType()
                    .add("name", StringType)
                    .add("id", LongType))
                .add("last_event", StringType))))

// COMMAND ----------

val nestDataDS2 = Seq("""{
    "devices": {
       "thermostats": {
          "peyiJNo0IldT2YlIVtYaGQ": {
            "device_id": "peyiJNo0IldT2YlIVtYaGQ",
            "locale": "en-US",
            "software_version": "4.0",
            "structure_id": "VqFabWH21nwVyd4RWgJgNb292wa7hG_dUwo2i2SG7j3-BOLY0BA4sw",
            "where_name": "Hallway Upstairs",
            "last_connection": "2016-10-31T23:59:59.000Z",
            "is_online": true,
            "can_cool": true,
            "can_heat": true,
            "is_using_emergency_heat": true,
            "has_fan": true,
            "fan_timer_active": true,
            "fan_timer_timeout": "2016-10-31T23:59:59.000Z",
            "temperature_scale": "F",
            "target_temperature_f": 72,
            "target_temperature_high_f": 80,
            "target_temperature_low_f": 65,
            "eco_temperature_high_f": 80,
            "eco_temperature_low_f": 65,
            "away_temperature_high_f": 80,
            "away_temperature_low_f": 65,
            "hvac_mode": "heat",
            "humidity": 40,
            "hvac_state": "heating",
            "is_locked": true,
            "locked_temp_min_f": 65,
            "locked_temp_max_f": 80
            }
          },
          "smoke_co_alarms": {
            "RTMTKxsQTCxzVcsySOHPxKoF4OyCifrs": {
              "device_id": "RTMTKxsQTCxzVcsySOHPxKoF4OyCifrs",
              "locale": "en-US",
              "software_version": "1.01",
              "structure_id": "VqFabWH21nwVyd4RWgJgNb292wa7hG_dUwo2i2SG7j3-BOLY0BA4sw",
              "where_name": "Jane's Room",
              "last_connection": "2016-10-31T23:59:59.000Z",
              "is_online": true,
              "battery_health": "ok",
              "co_alarm_state": "ok",
              "smoke_alarm_state": "ok",
              "is_manual_test_active": true,
              "last_manual_test_time": "2016-10-31T23:59:59.000Z",
              "ui_color_state": "gray"
              }
            },
         "cameras": {
          "awJo6rH0IldT2YlIVtYaGQ": {
            "device_id": "awJo6rH",
            "software_version": "4.0",
            "structure_id": "VqFabWH21nwVyd4RWgJgNb292wa7hG_dUwo2i2SG7j3-BOLY0BA4sw",
            "where_name": "Foyer",
            "is_online": true,
            "is_streaming": true,
            "is_audio_input_enabled": true,
            "last_is_online_change": "2016-12-29T18:42:00.000Z",
            "is_video_history_enabled": true,
            "web_url": "https://home.nest.com/cameras/device_id?auth=access_token",
            "app_url": "nestmobile://cameras/device_id?auth=access_token",
            "is_public_share_enabled": true,
            "activity_zones": { "name": "Walkway", "id": 244083 },
            "last_event": "2016-10-31T23:59:59.000Z"
            }
          }
        }
       }""").toDS

// COMMAND ----------

val nestDF2 = spark
            .read 
            .schema(nestSchema2)
            .json(nestDataDS2.rdd)

// COMMAND ----------

display(nestDF2)

// COMMAND ----------

val stringJsonDF = nestDF2.select(to_json(struct($"*"))).toDF("nestDevice")

// COMMAND ----------

stringJsonDF.printSchema

// COMMAND ----------

display(stringJsonDF)

// COMMAND ----------

val mapColumnsDF = nestDF2.select($"devices".getItem("smoke_co_alarms").alias ("smoke_alarms"),
                                  $"devices".getItem("cameras").alias ("cameras"),
                                  $"devices".getItem("thermostats").alias ("thermostats"))

// COMMAND ----------

display(mapColumnsDF)

// COMMAND ----------

val explodedThermostatsDF = mapColumnsDF.select(explode($"thermostats"))
val explodedCamerasDF = mapColumnsDF.select(explode($"cameras"))
val explodedSmokedAlarmsDF =  nestDF2.select(explode($"devices.smoke_co_alarms"))

// COMMAND ----------

display(explodedThermostatsDF)

// COMMAND ----------

val thermostateDF = explodedThermostatsDF.select($"value".getItem("device_id").alias("device_id"), 
                                                 $"value".getItem("locale").alias("locale"),
                                                 $"value".getItem("where_name").alias("location"),
                                                 $"value".getItem("last_connection").alias("last_connected"),
                                                 $"value".getItem("humidity").alias("humidity"),
                                                 $"value".getItem("target_temperature_f").alias("target_temperature_f"),
                                                 $"value".getItem("hvac_mode").alias("mode"),
                                                 $"value".getItem("software_version").alias("version"))

val cameraDF = explodedCamerasDF.select($"value".getItem("device_id").alias("device_id"),
                                        $"value".getItem("where_name").alias("location"),
                                        $"value".getItem("software_version").alias("version"),
                                        $"value".getItem("activity_zones").getItem("name").alias("name"),
                                        $"value".getItem("activity_zones").getItem("id").alias("id"))
                                         
val smokedAlarmsDF = explodedSmokedAlarmsDF.select($"value".getItem("device_id").alias("device_id"),
                                                   $"value".getItem("where_name").alias("location"),
                                                   $"value".getItem("software_version").alias("version"),
                                                   $"value".getItem("last_connection").alias("last_connected"),
                                                   $"value".getItem("battery_health").alias("battery_health"))
                                                  
                                        

// COMMAND ----------

display(thermostateDF)

// COMMAND ----------

display(cameraDF)

// COMMAND ----------

display(smokedAlarmsDF)

// COMMAND ----------

val joineDFs = thermostateDF.join(cameraDF, "version")


// COMMAND ----------

display(joineDFs)