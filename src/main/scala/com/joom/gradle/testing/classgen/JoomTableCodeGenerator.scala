package com.joom.gradle.testing.classgen

import org.apache.spark.sql.types.{DataType, StructField, StructType}

class JoomTableCodeGenerator extends SparkSchemaCodeGenerator[String] {

  def generateJoomTableCode(schema: StructType, tableName: String, database: String,
                            className: String, baseClassName: String, packageName: String): String = {
    // Generates a spark code needed to create the passed schema, like this:
    // StructType(Seq(
    //   StructField("id", StringType),
    //   StructField("name", StringType)
    // ))
    val schemaCode = generate(schema, initLevel = 1)

    s"""package $packageName
       |
       |import com.joom.warehouse.api.WarehousePublicConfig
       |import org.apache.spark.sql.types._
       |
       |object $className ${if (baseClassName == "") "" else s"extends $baseClassName"} {
       |
       |  // TODO: It is an auto-generated code. The logic for creating this table should be placed here.
       |
       |  override val tableName: String = "$tableName"
       |
       |  override def getDatabase(wpc: WarehousePublicConfig): String = wpc.hive${database.capitalize}Database
       |
       |  override val schema: StructType = $schemaCode
       |}
       |""".stripMargin
  }

  override protected def generateStructTypeElement(fieldElements: Seq[String], fieldName: String, level: Int): String = {
    s"StructType(Seq(\n${fieldElements.mkString(",\n")}\n${indent(level)}))"
  }

  override protected def generateFieldElement(field: StructField, dataTypeElement: String, level: Int): String = {
    val comment = field.getComment()
      .map { cm =>
        val q = if (cm.lines.size > 1) "\"\"\"" else "\""
        s".withComment($q$cm$q)"
      }
      .getOrElse("")

    s"""${indent(level)}StructField("${field.name}", $dataTypeElement)$comment"""
  }

  override protected def generateSimpleDataTypeElement(dataType: DataType): String = {
    dataType.toString
  }

  override protected def generateMapTypeElement(keyDataTypeElement: String, valueDataTypeElement: String): String = {
    s"MapType($keyDataTypeElement, $valueDataTypeElement)"
  }

  override protected def generateArrayTypeElement(dataTypeElement: String): String = {
    s"ArrayType($dataTypeElement)"
  }
}
