package mesosphere.marathon.core.launcher.impl

import java.util.Collections

import mesosphere.marathon.{ MarathonConf, MarathonSchedulerDriverHolder }
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.launcher.TaskLauncher
import mesosphere.marathon.metrics.{ MetricPrefixes, Metrics }
import org.apache.mesos.Protos.{ Offer, OfferID, Status, TaskInfo }
import org.apache.mesos.{ Protos, SchedulerDriver }
import org.slf4j.LoggerFactory

private[launcher] class TaskLauncherImpl(
    metrics: Metrics,
    marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
    conf: MarathonConf,
    clock: Clock) extends TaskLauncher {
  private[this] val log = LoggerFactory.getLogger(getClass)
  private[this] lazy val principal: String = conf.mesosAuthenticationPrincipal.get.getOrElse(throw new RuntimeException("no frameworkPrincipal set"))

  private[this] val usedOffersMeter = metrics.meter(metrics.name(MetricPrefixes.SERVICE, getClass, "usedOffers"))
  private[this] val launchedTasksMeter = metrics.meter(metrics.name(MetricPrefixes.SERVICE, getClass, "launchedTasks"))
  private[this] val declinedOffersMeter =
    metrics.meter(metrics.name(MetricPrefixes.SERVICE, getClass, "declinedOffers"))

  // TODO: rename – need to use acceptOffers with operations instead of launchTasks
  override def launchTasks(offerID: OfferID, taskInfos: Seq[TaskInfo]): Boolean = {
    val launched = withDriver(s"launchTasks($offerID)") { driver =>
      import scala.collection.JavaConverters._
      if (log.isDebugEnabled) {
        log.debug(s"Launching tasks on $offerID:\n${taskInfos.mkString("\n")}")
      }

      val operations = taskInfos.flatMap { taskInfo =>
        // FIXME: right now we just always reserve and create to test functionality
        Seq(
          Operation.reserveResources(principal),
          Operation.createVolume(principal),
          Operation.launch(taskInfo))
      }
      driver.acceptOffers(Collections.singleton(offerID), operations.asJava, Protos.Filters.newBuilder().build())
    }
    if (launched) {
      usedOffersMeter.mark()
      launchedTasksMeter.mark(taskInfos.size.toLong)
    }
    launched
  }

  override def declineOffer(offerID: OfferID, refuseMilliseconds: Option[Long]): Unit = {
    val declined = withDriver(s"declineOffer(${offerID.getValue})") {
      val filters = refuseMilliseconds
        .map(seconds => Protos.Filters.newBuilder().setRefuseSeconds(seconds / 1000.0).build())
        .getOrElse(Protos.Filters.getDefaultInstance)
      _.declineOffer(offerID, filters)
    }
    if (declined) {
      declinedOffersMeter.mark()
    }
  }

  private[this] def withDriver(description: => String)(block: SchedulerDriver => Status): Boolean = {
    marathonSchedulerDriverHolder.driver match {
      case Some(driver) =>
        val status = block(driver)
        if (log.isDebugEnabled) {
          log.debug(s"$description returned status = $status")
        }
        status == Status.DRIVER_RUNNING

      case None =>
        log.warn(s"Cannot execute '$description', no driver available")
        false
    }
  }
}

// ----------------------------------------------------------------
//        Quick and Dirty to test functionality
// ----------------------------------------------------------------

object TestValues {
  val role = "foo"
  val persistentVolumeId = "myPersistentVolumeId"
  val containerPath = "/my/containerPath"
}

object Operation {

  def reserveResources(principal: String): Offer.Operation = {
    val disk = Protos.Resource.newBuilder()
      .setName("disk")
      .setType(Protos.Value.Type.SCALAR)
      .setScalar(Protos.Value.Scalar.newBuilder().setValue(10).build())
      .setRole(TestValues.role)
      .setReservation(Protos.Resource.ReservationInfo.newBuilder().setPrincipal(principal).build())
      .build()

    val reserve = Offer.Operation.Reserve.newBuilder()
      .addResources(disk)
      .build()

    Offer.Operation.newBuilder()
      .setType(Protos.Offer.Operation.Type.RESERVE)
      .setReserve(reserve)
      .build()
  }

  def createVolume(principal: String): Offer.Operation = {
    import org.apache.mesos.Protos.Offer
    val disk = {
      val persistence = Protos.Resource.DiskInfo.Persistence.newBuilder()
        .setId("myPersistentVolume")
        .build()

      val volume = Protos.Volume.newBuilder()
        .setContainerPath(TestValues.containerPath)
        .setMode(Protos.Volume.Mode.RW)
        .build()

      Protos.Resource.DiskInfo.newBuilder()
        .setPersistence(persistence)
        .setVolume(volume)
        .build()
    }

    val resource = Protos.Resource.newBuilder()
      .setName("disk")
      .setType(Protos.Value.Type.SCALAR)
      .setScalar(Protos.Value.Scalar.newBuilder().setValue(10).build())
      .setRole(TestValues.role)
      .setReservation(Protos.Resource.ReservationInfo.newBuilder().setPrincipal(principal).build())
      .setDisk(disk)

    val create = Offer.Operation.Create.newBuilder()
      .addVolumes(resource)
      //      .setVolumes(0, resource)
      .build()

    Offer.Operation.newBuilder()
      .setType(Protos.Offer.Operation.Type.CREATE)
      .setCreate(create)
      .build()
  }

  def launch(taskInfo: Protos.TaskInfo): Offer.Operation = {

    val launch = Offer.Operation.Launch.newBuilder()
      .addTaskInfos(taskInfo)
      .build()

    Offer.Operation.newBuilder()
      .setType(Protos.Offer.Operation.Type.LAUNCH)
      .setLaunch(launch)
      .build()
  }

}
