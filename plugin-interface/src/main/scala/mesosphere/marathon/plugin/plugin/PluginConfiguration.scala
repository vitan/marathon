package mesosphere.marathon.plugin.plugin

import play.api.libs.json.JsObject

/**
  * Plugin can be extended to receive configuration from plugin descriptor.
  */
trait PluginConfiguration { self: Plugin =>

  /**
    * If a plugin implements this trait, it gets initialized with a configuration
    * defined in the plugin descriptor.
    *
    * @param frameworkName the Framework name Marathon used to register with Mesos.
    * @param configuration the json configuration from the plugin descriptor.
    */
  def initialize(frameworkName: String, configuration: JsObject): Unit

}
