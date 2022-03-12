package com.joom.gradle.testing.classgen

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util

import org.apache.spark.sql.types.StructType

import collection.JavaConverters._
import org.gradle.api.DefaultTask

trait CreateTableClassesTask extends DefaultTask {

  private var params: util.ArrayList[CreateTableClassesParam] = _
  def params(params: util.ArrayList[CreateTableClassesParam]): CreateTableClassesTask = {
    this.params = params
    this
  }

  protected def generateCode(schema: StructType, tableName: String, database: String,
                             className: String, baseClassName: String, packageName: String): String

  protected val overwriteExistingFiles: Boolean = true

  protected def createClasses(): Unit = {
    if (params == null || params.isEmpty)
      throw new IllegalArgumentException("Property 'params' must be set")

    val createClassesParams = params.asScala.toArray
    createClassesParams.foreach(_.checkParams())

    using(new TableSchemaStoreClient()) { tableSchemaStoreClient =>
      createClassesParams.foreach { p =>
        val targetPath = new File("src/main/scala/").getAbsoluteFile.toPath
          .resolve(p.getTargetPackage.replace('.', '/'))
        Files.createDirectories(targetPath)

        p.getTables.foreach { table =>
          try {
            val className = table.split('_').map(_.capitalize).mkString
            val classLocation = targetPath.resolve(s"$className.scala")

            if (classLocation.toFile.exists() && !overwriteExistingFiles) {
              getLogger.warn(s"File $classLocation already exists. This task doesn't overwrite existing files. " +
                s"You must explicitly delete it first.")
            } else {
              val schema = tableSchemaStoreClient.getSparkSchema(p.getDatabase, table)
              val code = generateCode(schema, table, p.getDatabase, className, p.getBaseClassName, p.getTargetPackage)

              Files.write(classLocation, code.getBytes(StandardCharsets.UTF_8))
            }
          } catch {
            case ex: Throwable =>
              getLogger.error(s"Failed to generate a class for database = '${p.getDatabase}', table = '$table'", ex)
              throw ex
          }
        }
      }
    }
  }
}
