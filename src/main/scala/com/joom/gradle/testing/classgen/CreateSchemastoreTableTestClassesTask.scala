package com.joom.gradle.testing.classgen

import org.apache.spark.sql.types.StructType
import org.gradle.api.tasks.TaskAction

/**
 * <p>Generates test case-classes for the specified tables located in database.
 * Tables info is taken from Hive-metastore using PlatformManager api
 * (<i>http://schemastore.joom.a/api/tables/:fulltablename</i>).</p>
 *
 * Usage example in build.gradle:
 * <pre><i>
 * task createSchemastoreTableTestClasses(type: CreateSchemastoreTableTestClassesTask) {
 *   params([
 *     new CreateTableClassesParam()
 *       .setTargetPackage("com.joom.warehouse.testing.tables.mart")
 *       .setDatabase("mart")
 *       .setTables(["table"] as ArrayList<String>)
 *   ] as ArrayList<CreateTableClassesParam>)
 * }
 * </i></pre>
 */
class CreateSchemastoreTableTestClassesTask extends CreateTableClassesTask {

  private val codegen = new CaseClassesCodeGenerator(makeAllFieldsOptional = true)

  @TaskAction
  def start(): Unit = {
    createClasses()
  }

  override protected def generateCode(schema: StructType, tableName: String, database: String,
                                      className: String, baseClassName: String, packageName: String): String = {
    codegen.generateCaseClassCode(schema, className, packageName, baseClassName)
  }
}
