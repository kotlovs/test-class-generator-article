package com.joom.gradle.testing.classgen

import org.gradle.api.tasks.TaskAction
import org.apache.spark.sql.types.StructType

/**
 * <p>This task generates initial JoomTable classes for the specified Spark tables.</p>
 * Tables info is taken from Hive-metastore using PlatformManager api
 * (<i>http://schemastore.joom.a/api/tables/:fulltablename</i>).
 * If a class for the specified table already exists, it will not be overwritten, since it may contain real code.
 * To overwrite the existing file, you must explicitly delete it first.
 * <p>Generated files will look like this:</p>
 * <pre><i>
 * object TableName extends JoomTable {
 *   override val tableName: String = "table_name"
 *
 *   override val database: String = "database"
 *
 *   override val schema: StructType = StructType(Seq(
 *     StructField("id", StringType),
 *     StructField("name", StringType)
 * 	   .....
 *   ))
 * }
 * </i></pre>
 */
class CreateJoomTableInitialClassesTask extends CreateTableClassesTask {

  private val codegen = new JoomTableCodeGenerator()

  @TaskAction
  def start(): Unit = {
    createClasses()
  }

  override protected val overwriteExistingFiles: Boolean = false

  override protected def generateCode(schema: StructType, tableName: String, database: String,
                                      className: String, baseClassName: String, packageName: String): String = {
    codegen.generateJoomTableCode(schema, tableName, database, className, baseClassName, packageName)
  }
}
