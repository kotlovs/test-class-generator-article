package com.joom.gradle.testing.classgen

import java.io.File
import java.net.URLClassLoader
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import com.google.common.reflect.ClassPath
import org.apache.spark.sql.types.StructType
import org.gradle.api.DefaultTask
import org.gradle.api.tasks.TaskAction
import scala.reflect.runtime.universe._

/**
 * <p>Generates test case-classes for all classes implementing the <i>baseJoomTableClass</i> (such class should contain
 * a schema describing spark table) and located in <i>inputJarPath</i>.</p>
 *
 * Usage example in build.gradle:
 * <pre><i>
 * task createJoomTableTestClasses(type: CreateJoomTableTestClassesTask) {
 *   dependsOn shadowJar // to build the actual version of full jar with :warehouse classes
 *   inputJarPath "build/libs/ *.jar"
 *   packageToSearch "com.joom.warehouse"
 *   baseJoomTableClass "com.joom.warehouse.JoomTable"
 *   targetPackage "com.joom.warehouse.testing.tables"
 * }
 * </i></pre>
 */
class CreateJoomTableTestClassesTask extends DefaultTask {

  private var inputJarPath: String = _
  /**
   * @param inputJarPath  path to *.jar, where classes with Spark schemes are located. At the moment, this jar should
   *                      also contain the PlatformApi stuff.
   * @return this
   */
  def inputJarPath(inputJarPath: String): CreateJoomTableTestClassesTask = {
    this.inputJarPath = inputJarPath
    this
  }

  private var packageToSearch: String = _
  /**
   * @param packageToSearch  root package, where classes implementing the <i>baseJoomTableClass</i> will be sought
   * @return this
   */
  def packageToSearch(packageToSearch: String): CreateJoomTableTestClassesTask = {
    this.packageToSearch = packageToSearch
    this
  }

  private var baseJoomTableClass: String = _
  /**
   * @param baseJoomTableClass  test case-classes will be created only for classes implementing this interface
   * @return this
   */
  def baseJoomTableClass(baseJoomTableClass: String): CreateJoomTableTestClassesTask = {
    this.baseJoomTableClass = baseJoomTableClass
    this
  }

  private var targetPackage: String = _
  /**
   * @param targetPackage  package in the current module, where the generated classes will be placed
   * @return this
   */
  def targetPackage(targetPackage: String): CreateJoomTableTestClassesTask = {
    this.targetPackage = targetPackage
    this
  }

  @TaskAction
  def start(): Unit = {
    if (inputJarPath == null)
      throw new IllegalArgumentException("Property 'inputJarPath' must be set")

    if (packageToSearch == null)
      throw new IllegalArgumentException("Property 'packageToSearch' must be set")

    if (baseJoomTableClass == null)
      throw new IllegalArgumentException("Property 'baseJoomTableClass' must be set")

    if (targetPackage == null)
      throw new IllegalArgumentException("Property 'targetPackage' must be set")

    val baseTargetPath = new File("src/main/scala/").getAbsoluteFile.toPath
      .resolve(targetPackage.replace('.', '/'))

    if (!baseTargetPath.toFile.exists())
      throw new IllegalArgumentException(s"TargetPath '$baseTargetPath' doesn't exist. " +
        s"Check the property 'targetPackage' ($targetPackage)")

    val inputPath = new File(inputJarPath)
    if (!inputPath.exists())
      throw new IllegalArgumentException(s"inputJarPath '$inputJarPath' doesn't exist")

    using(getClassLoader(inputPath)) { classLoader =>
      val classNames = listAllClasses(classLoader, packageToSearch)
      val joomTables = findJoomTables(classNames, baseJoomTableClass, classLoader)

      val cg = new CaseClassesCodeGenerator(makeAllFieldsOptional = true)

      joomTables.foreach { joomTable =>
        val className = joomTable.tableName.split('_').map(_.capitalize).mkString
        val packageName = s"$targetPackage.${joomTable.database}"
        val targetPath = baseTargetPath.resolve(joomTable.database)
        Files.createDirectories(targetPath)

        val caseClassCode = cg.generateCaseClassCode(joomTable.schema, className, packageName)

        Files.write(targetPath.resolve(s"$className.scala"),
          caseClassCode.getBytes(StandardCharsets.UTF_8))
      }
    }
  }

  private def findJoomTables(classNames: Set[String], baseClassName: String, classLoader: URLClassLoader)
  : Set[JoomTableInfo] = {
    val mirror = runtimeMirror(classLoader)
    val baseClass = classLoader.loadClass(baseClassName)
    val baseClassSymbol = mirror.classSymbol(baseClass)
    val wpc = getWarehousePublicConfig(mirror)

    classNames.flatMap { className =>
      try {
        val module = mirror.staticModule(className)

        if (module.moduleClass.asClass.baseClasses.contains(baseClassSymbol)) {
          val instance = mirror.reflectModule(module).instance

          def invokeMethod(name: String) = instance.getClass
            .getMethod(name)
            .invoke(instance)

          val tableName = invokeMethod("tableName").asInstanceOf[String]
          val schema = invokeMethod("schema").asInstanceOf[StructType]
          // take the database from the production configuration
          val database = instance.getClass
            .getMethod("getDatabase", wpc.getClass)
            .invoke(instance, wpc).asInstanceOf[String]
          Some(JoomTableInfo(tableName, database, schema))
        } else
          None
      } catch {
        case ex: Throwable => {
          // There are a small number of classes (f.e. HyperLogLog) that sometimes refuse to load, but we don’t need them.
          getLogger.warn(s"Class $className can't be loaded: $ex")
          None
        }
      }
    }
  }

  private def listAllClasses(classLoader: URLClassLoader, packageToSearch: String): Set[String] = {
    import collection.JavaConverters._
    val cp = ClassPath.from(classLoader)
    cp.getTopLevelClassesRecursive(packageToSearch).asScala.map(_.getName).toSet
  }

  private def getClassLoader(path: File) = {
    new URLClassLoader(Array(path.toURI.toURL), this.getClass.getClassLoader)
  }

  private def getWarehousePublicConfig(mirror: Mirror) = {
    val platformApiMod = mirror.staticModule("com.joom.warehouse.api.PlatformApi")
    val platformApiInst = mirror.reflectModule(platformApiMod).instance
    val productionConfig = platformApiInst.getClass
      .getMethod("productionConfig", classOf[String], classOf[String])
      .invoke(platformApiInst, "platform", "job")
    productionConfig.getClass
      .getMethod("publicConfig")
      .invoke(productionConfig)
  }

  case class JoomTableInfo(tableName: String, database: String, schema: StructType)
}
