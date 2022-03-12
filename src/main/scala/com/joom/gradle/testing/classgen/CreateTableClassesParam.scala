package com.joom.gradle.testing.classgen

import java.util
import collection.JavaConverters._

class CreateTableClassesParam {

  private var targetPackage: String = _
  def getTargetPackage: String = targetPackage

  /**
   * @param targetPackage  package in the current module, where the generated classes will be placed
   * @return this
   */
  def setTargetPackage(targetPackage: String): CreateTableClassesParam = {
    this.targetPackage = targetPackage
    this
  }

  private var database: String = _
  def getDatabase: String = database

  /**
   * @param database  database where tables are located
   * @return this
   */
  def setDatabase(database: String): CreateTableClassesParam = {
    this.database = database
    this
  }

  private var tables: util.ArrayList[String] = _
  def getTables: Iterable[String] = tables.asScala

  /**
   * @param tables  for these tables will be generated test case classes
   * @return this
   */
  def setTables(tables: util.ArrayList[String]): CreateTableClassesParam = {
    this.tables = tables
    this
  }

  private var baseClassName: String = "" // has default
  def getBaseClassName: String = baseClassName

  /**
   * @param baseClassName  generated classes will extend this class (if provided)
   * @return this
   */
  def setBaseClassName(baseClassName: String): CreateTableClassesParam = {
    this.baseClassName = baseClassName
    this
  }

  @throws[IllegalArgumentException]
  def checkParams(): Unit = {

    if (targetPackage == null || targetPackage.isEmpty)
      throw new IllegalArgumentException("Property 'targetPackage' must be set")

    if (database == null || database.isEmpty)
      throw new IllegalArgumentException("Property 'database' must be set")

    if (tables == null || tables.isEmpty || tables.asScala.exists(_.isEmpty))
      throw new IllegalArgumentException("Property 'tables' must be set and not empty")
  }
}
