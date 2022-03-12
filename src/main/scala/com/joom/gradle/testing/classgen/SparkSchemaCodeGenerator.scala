package com.joom.gradle.testing.classgen

import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}

/**
 * Recursively traverses the elements of spark schema and calls methods for generating the corresponding elements.
 * This methods must be overridden in subclass.
 */
trait SparkSchemaCodeGenerator[T] {

  protected def generate(schema: StructType, name: String = "", initLevel: Int = 0): T = {
    generateStructTypeElement(schema, name, initLevel)
  }

  protected def generateStructTypeElement(fieldElements: Seq[T], fieldName: String, level: Int): T

  protected def generateFieldElement(field: StructField, dataTypeElement: T, level: Int): T

  protected def generateSimpleDataTypeElement(dataType: DataType): T

  protected def generateMapTypeElement(keyDataTypeElement: T, valueDataTypeElement: T): T

  protected def generateArrayTypeElement(dataTypeElement: T): T

  protected val defaultIndent = 2
  protected def indent(level: Int): String = " " * level * defaultIndent

  private def generateStructTypeElement(structType: StructType, fieldName: String, level: Int): T = {

    val fieldElements = structType.map { field =>
      generateFieldElement(field, level + 1)
    }

    generateStructTypeElement(fieldElements, fieldName, level): T
  }

  private def generateFieldElement(field: StructField, level: Int): T = {
    val dataTypeElement = generateDataTypeElement(field.dataType, field.name, level)

    generateFieldElement(field: StructField, dataTypeElement: T, level: Int)
  }

  private def generateDataTypeElement(dataType: DataType, fieldName: String, level: Int): T = {
    dataType match {
      case dt: StructType => generateStructTypeElement(dt, fieldName, level)
      case dt: MapType => generateMapTypeElement(
        generateDataTypeElement(dt.keyType, s"${fieldName}_key", level),
        generateDataTypeElement(dt.valueType, s"${fieldName}_value", level))
      case dt: ArrayType => generateArrayTypeElement(generateDataTypeElement(dt.elementType, fieldName, level))
      case dt: DataType => generateSimpleDataTypeElement(dt)
    }
  }
}
