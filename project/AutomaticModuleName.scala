import sbt.{Def, _}
import sbt.Keys._

object AutomaticModuleName {
  private val AutomaticModuleName = "Automatic-Module-Name"

  def settings(name: String): Seq[Def.Setting[Task[Seq[PackageOption]]]] = Seq(
    packageOptions in (Compile, packageBin) += Package.ManifestAttributes(AutomaticModuleName -> name)
  )
}
