package mesosphere.marathon.core.matcher.base.util

import mesosphere.marathon.WrongConfigurationException
import mesosphere.marathon.core.task.Task.LocalVolume
import mesosphere.marathon.state.PathId
import org.apache.mesos.Protos.Resource.ReservationInfo
import org.apache.mesos.{ Protos => Mesos }
import org.slf4j.LoggerFactory

class OfferOperationFactory(
    private val principalOpt: Option[String],
    private val roleOpt: Option[String]) {

  private[this] val log = LoggerFactory.getLogger(getClass)

  private[this] lazy val principal: String = principalOpt match {
    case Some(value) => value
    case _ => throw new WrongConfigurationException(
      "No principal set. Set --mesos_authentication_principal to enable using local volumes in Marathon.")
  }

  private[this] lazy val role: String = roleOpt match {
    case Some(value) => value
    case _ => throw new WrongConfigurationException(
      "No principal set. Set --mesos_role to enable using local volumes in Marathon.")
  }

  /** Create a launch operation for the given taskInfo. */
  def launch(taskInfo: Mesos.TaskInfo): Mesos.Offer.Operation = {
    val launch = Mesos.Offer.Operation.Launch.newBuilder()
      .addTaskInfos(taskInfo)
      .build()

    Mesos.Offer.Operation.newBuilder()
      .setType(Mesos.Offer.Operation.Type.LAUNCH)
      .setLaunch(launch)
      .build()
  }

  def reserve(resources: Iterable[Mesos.Resource]): Mesos.Offer.Operation = {
    import scala.collection.JavaConverters._
    val reservedResources = resources.map { resource =>
      Mesos.Resource.newBuilder(resource)
        .setRole(role) // actually needed or already in the resource?
        .setReservation(ReservationInfo.newBuilder()
          .setPrincipal(principal)).build();
    }
    val reserve = Mesos.Offer.Operation.Reserve.newBuilder()
      .addAllResources(reservedResources.asJava)
      .build()

    Mesos.Offer.Operation.newBuilder()
      .setType(Mesos.Offer.Operation.Type.RESERVE)
      .setReserve(reserve)
      .build()
  }

  def createVolumes(appId: PathId, localVolumes: Iterable[LocalVolume]): Mesos.Offer.Operation = {
    import scala.collection.JavaConverters._

    val volumes: Iterable[Mesos.Resource] = localVolumes.map { vol =>
      val disk = {
        val persistence = Mesos.Resource.DiskInfo.Persistence.newBuilder()
          .setId(vol.id.idString)

        val volume = Mesos.Volume.newBuilder()
          .setContainerPath(vol.persistentVolume.containerPath)
          .setMode(vol.persistentVolume.mode)

        Mesos.Resource.DiskInfo.newBuilder()
          .setPersistence(persistence)
          .setVolume(volume)
      }

      Mesos.Resource.newBuilder()
        .setName("disk")
        .setType(Mesos.Value.Type.SCALAR)
        .setScalar(Mesos.Value.Scalar.newBuilder().setValue(vol.persistentVolume.persistent.size.toDouble).build())
        .setRole(role)
        .setReservation(Mesos.Resource.ReservationInfo.newBuilder().setPrincipal(principal).build())
        .setDisk(disk)
        .build()
    }

    val create = Mesos.Offer.Operation.Create.newBuilder()
      .addAllVolumes(volumes.asJava)

    Mesos.Offer.Operation.newBuilder()
      .setType(Mesos.Offer.Operation.Type.CREATE)
      .setCreate(create)
      .build()
  }

}