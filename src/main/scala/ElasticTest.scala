/**
 * Created by andrewwang on 11/5/15.
 */

import java.util.UUID

import com.sksamuel.elastic4s.analyzers.KeywordAnalyzer
import com.sksamuel.elastic4s.{IndexResult, RichSearchHit, ElasticsearchClientUri, ElasticClient}
import org.elasticsearch.common.settings.Settings
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.mappings.FieldType._

import RandomMetricGenerator._
import ElasticAdapterConversions._

import scala.concurrent.duration._


object ElasticTest extends App {
  val host = "127.0.0.1"
  val elastic = new ElasticAdapter(host)
  val devId = UUID.fromString("5203869e-3c38-413b-889c-af9b7721bea4")
  val groupId = UUID.fromString("7d136a6c-a6ad-4c8c-9513-f32e5b145224")
  val numMetrics = 10

  elastic.writeMapping()

  (0 to numMetrics).foreach {
    i => {
      elastic.writeDeviceMetric(randomDeviceMetric(devId, randomTimestampInLast(1 minutes)))
      println(s"Wrote device metric ${i}")

      elastic.writeGroupMetric(randomGroupMetric(groupId, randomTimestampInLast(1 minutes)))
      println(s"Wrote group metric ${i}")
    }
  }

  Thread.sleep(3000l)
  val currTs = System.currentTimeMillis()

  // Query device metrics across a 30s timewindow
  println(elastic.queryDeviceMetric(devId, currTs - 60000l, currTs))

  // Query group metrics across a 30s timewindow
  println(elastic.queryGroupMetric(groupId, currTs - 60000l, currTs))

}


class ElasticAdapter(host: String) {

//  val settings = Settings.settingsBuilder().put("http.enabled", true)
//    .put("path.home", "/Users/andrewwang/elasticsearch").build()
//  val client = ElasticClient.local(settings)
  val settings = Settings.settingsBuilder().put("cluster.name", "elasticsearch").build()
  val client = ElasticClient.transport(settings, s"elasticsearch://${host}:9300")

  val queryTimeout = 5.seconds
  val groupIndex = "group"
  val deviceIndex = "device"
  val groupDocument = "GroupMetric"
  val deviceDocument = "DeviceMetric"

  def writeMapping() = {
    client.execute {
      create index deviceIndex mappings(
        deviceDocument all false,
        deviceDocument source false,
        deviceDocument fields(
          "deviceId" typed StringType analyzer KeywordAnalyzer store true includeInAll true,
          "timestamp" typed LongType,
          "inputPowerLimitW" typed DoubleType index "no" docValuesFormat true,
          "inputPowerW" typed DoubleType index "no" docValuesFormat true,
          "inputCurrentA" typed DoubleType index "no" docValuesFormat true,
          "inputVoltageV" typed DoubleType index "no" docValuesFormat true,
          "battPowerOutW" typed DoubleType index "no" docValuesFormat true,
          "battCurrentA" typed DoubleType index "no" docValuesFormat true,
          "battVoltageV" typed DoubleType index "no" docValuesFormat true,
          "battTempC" typed DoubleType index "no" docValuesFormat true,
          "battSocWh" typed DoubleType index "no" docValuesFormat true,
          "battSocProp" typed DoubleType index "no" docValuesFormat true,
          "outputPowerW" typed DoubleType index "no" docValuesFormat true,
          "outputCurrentA" typed DoubleType index "no" docValuesFormat true,
          "outputVoltageV" typed DoubleType index "no" docValuesFormat true
          )
        )

    }

    client.execute {
      create index groupIndex mappings(
        groupDocument all false,
        groupDocument source false,
        groupDocument fields(
          "groupId" typed StringType analyzer KeywordAnalyzer store true includeInAll true,
          "timestamp" typed LongType,
          "powerLimitW" typed StringType index "no" docValuesFormat true,
          "inputPowerW" typed StringType index "no" docValuesFormat true,
          "outputPowerW" typed StringType index "no" docValuesFormat true,
          "battDischargePowerW" typed StringType index "no" docValuesFormat true,
          "battChargePowerW" typed StringType index "no" docValuesFormat true,
          "battSocWh" typed StringType index "no" docValuesFormat true,
          "battSocPropAvg" typed StringType index "no" docValuesFormat true,
          "battLowestSocUpsTime" typed StringType index "no" docValuesFormat true,
          "numBlocksCharging" typed StringType index "no" docValuesFormat true,
          "numBlocksDischarging" typed StringType index "no" docValuesFormat true
          )
        )
    }
  }

  def writeGroupMetric(g: GroupMetric): Unit = {
    client.execute {
      index into groupIndex -> groupDocument doc g
    }
  }

  def writeDeviceMetric(d: DeviceMetric): Unit = {
    val respFuture = client.execute {
      index into deviceIndex -> deviceDocument doc d
    }
  }

  def queryDeviceMetric(deviceId: UUID, startTs: Long, endTs: Long): Seq[DeviceMetric]= {
    val resp = client.execute {
      search in deviceIndex -> deviceDocument query
        bool(
          must(
            Seq(termQuery("deviceId", deviceId.toString),
              rangeQuery("timestamp") from startTs to endTs))
        )
    }.await(queryTimeout)   // FIXME: Do not block in real code
    val ret = resp.as[DeviceMetric]   // FIXME: Handle failures
    ret
  }

  def queryGroupMetric(groupId: UUID, startTs: Long, endTs: Long): Seq[GroupMetric]= {
    val resp = client.execute {
      search in groupIndex -> groupDocument query
        bool(
          must(
            Seq(termQuery("groupId", groupId.toString),
              rangeQuery("timestamp") from startTs to endTs))
        )
    }.await(queryTimeout)   // FIXME: Do not block in real code
    val ret = resp.as[GroupMetric] // FIXME: Handle failures
    ret
  }
}
