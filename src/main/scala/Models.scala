import java.util.UUID

import com.sksamuel.elastic4s.source.DocumentMap

import scala.concurrent.duration.FiniteDuration
import scala.util.Random._

case class DeviceMetric(
                         val deviceId: UUID,
                         val timestamp: Long,
                         val inputPowerLimitW: Double,
                         val inputPowerW: Double,
                         val inputCurrentA: Double,
                         val inputVoltageV: Double,
                         val battPowerOutW: Double,
                         val battCurrentA: Double,
                         val battVoltageV: Double,
                         val battTempC: Double,
                         val battSocWh: Double,
                         val battSocProp: Double,
                         val outputPowerW: Double,
                         val outputCurrentA: Double,
                         val outputVoltageV: Double) extends DocumentMap {
  def map = Map("deviceId" -> deviceId,
    "timestamp" -> timestamp,
    "inputPowerLimitW" -> inputPowerLimitW,
    "inputPowerW" -> inputPowerW,
    "inputCurrentA" -> inputCurrentA,
    "inputVoltageV" -> inputVoltageV,
    "battPowerOutW" -> battPowerOutW,
    "battCurrentA" -> battCurrentA,
    "battVoltageV" -> battVoltageV,
    "battTempC" -> battTempC,
    "battSocWh" -> battSocWh,
    "battSocProp" -> battSocProp,
    "outputPowerW" -> outputPowerW,
    "outputCurrentA" -> outputCurrentA,
    "outputVoltageV" -> outputVoltageV)
}

case class GroupMetric(
                         val groupId: UUID,
                         val timestamp: Long,
                         val powerLimitW: Double,
                         val inputPowerW: Double,
                         val outputPowerW: Double,
                         val battDischargePowerW: Double,
                         val battChargePowerW: Double,
                         val battSocWh: Double,
                         val battSocPropAvg: Double,
                         val battLowestSocUpsTime: Double,
                         val numBlocksCharging: Long,
                         val numBlocksDischarging: Long) extends DocumentMap {
  def map = Map(
    "groupId" -> groupId,
    "timestamp" -> timestamp,
    "powerLimitW" -> powerLimitW,
    "inputPowerW" -> inputPowerW,
    "outputPowerW" -> outputPowerW,
    "battDischargePowerW" -> battDischargePowerW,
    "battChargePowerW" -> battChargePowerW,
    "battSocWh" -> battSocWh,
    "battSocPropAvg" -> battSocPropAvg,
    "battLowestSocUpsTime" -> battLowestSocUpsTime,
    "numBlocksCharging" -> numBlocksCharging,
    "numBlocksDischarging" -> numBlocksDischarging)
}

object RandomMetricGenerator {

  def randomGroupMetric(id: UUID, ts: Long): GroupMetric = {
    new GroupMetric(
      groupId = id,
      timestamp = ts,
      powerLimitW = nextDouble(),
      inputPowerW = nextDouble(),
      outputPowerW = nextDouble(),
      battDischargePowerW = nextDouble(),
      battChargePowerW = nextDouble(),
      battSocWh = nextDouble(),
      battSocPropAvg = nextDouble(),
      battLowestSocUpsTime = nextDouble(),
      numBlocksCharging = nextLong(),
      numBlocksDischarging = nextLong())
  }

  def randomDeviceMetric(id: UUID, ts: Long): DeviceMetric = {
    new DeviceMetric(
      deviceId = id,
      timestamp = ts,
      inputPowerLimitW = nextDouble(),
      inputPowerW = nextDouble(),
      inputCurrentA = nextDouble(),
      inputVoltageV = nextDouble(),
      battPowerOutW = nextDouble(),
      battCurrentA = nextDouble(),
      battVoltageV = nextDouble(),
      battTempC = nextDouble(),
      battSocWh = nextDouble(),
      battSocProp = nextDouble(),
      outputPowerW = nextDouble(),
      outputCurrentA = nextDouble(),
      outputVoltageV = nextDouble()
    )
  }

  def randomTimestampInLast(d: FiniteDuration) = {
    System.currentTimeMillis() - scala.math.round(nextDouble() * d.toMillis)
  }
}