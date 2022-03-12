package com.joom.gradle.testing.classgen

import com.joom.gradle.testing.classgen.CaseClassesCodeGenerator.Method
import org.apache.spark.sql.types._
import javax.lang.model.SourceVersion

/**
 * <p>Generates test case classes according to Spark schemas.</p>
 * The StructType can be complex with various combinations of ArrayType, MapType, StructType.
 * Therefore, RootClass is created and all nested StructTypes are also wrapped in their case classes named after
 * the names of the corresponding fields. All nested case classes are placed in 'object RootClass' to avoid name
 * conflicts with others classes (generated for others StructTypes and accidentally placed in the same package).
 *
 * <p>Example:</p>
 * <pre><i>
 * case class ProductPurchasePayload (
 *   categoryId: Option[String] = None,
 *   discountInfo: Option[ProductPurchasePayload.DiscountInfo] = None,
 *   ......
 * ) extends DeviceEventPayload {
 *   override def getEventType: String = "productPurchase"
 * }
 *
 * object ProductPurchasePayload {
 *   case class DiscountInfo (
 *     discounts: Option[Seq[ProductPurchasePayload.Discounts]] = None,
 *     pointsCashback: Option[ProductPurchasePayload.PointsCashback] = None,
 *     pointsCashbackFraction: Option[Double] = None,
 *     ........
 *   )
 *
 *   case class Discounts (
 *     amount: Option[Double] = None,
 *     ........
 *   )
 *
 *   case class PointsCashback (
 *     value: Option[Long] = None,
 *     ........
 *   )
 * }
 * </i></pre>
 */
class CaseClassesCodeGenerator(makeAllFieldsOptional: Boolean = true) extends SparkSchemaCodeGenerator[String] {

  private val caseClasses = scala.collection.mutable.Map[String, String]()

  private val rootClassNamePlaceholder = "__rootClassNamePlaceholder__"

  def generateCaseClassCode(schema: StructType, className: String, packageName: String,
                            baseClassName: String = "", methods: Seq[Method] = Seq()): String = {
    assert(caseClasses.isEmpty)
    // return only root className, all generated case classes are contained in 'caseClasses' field
    val rootClassName = generate(schema, className)
      .replace(rootClassNamePlaceholder, "")

    val rootClass = caseClasses(rootClassName)
      .replace(rootClassNamePlaceholder, rootClassName + ".")

    val nestedClasses = caseClasses.filterKeys(_ != rootClassName).values
      .map(_.replace(rootClassNamePlaceholder, rootClassName + "."))

    caseClasses.clear()

    s"package $packageName\n$rootClass ${if (baseClassName == "") "" else s"extends $baseClassName"}\n" +
      (if (methods.nonEmpty)
        s"""{
           |${methods.map(m => s"  ${m.generateCode}").mkString("\n\n")}
           |}
           |""".stripMargin
      else
        "") +
      (if (nestedClasses.nonEmpty)
        s"""
           |object $rootClassName {
           |${nestedClasses.mkString("\n")}
           |}
           |""".stripMargin
      else
        "")
  }

  override protected def generateStructTypeElement(fieldElements: Seq[String], fieldName: String, level: Int): String = {
    val className = createCaseClassName(fieldName)

    val classBody = s"""
                       |case class $className (
                       |${fieldElements.map(f => s"${indent(1)}$f").mkString(",\n")}
                       |)""".stripMargin

    caseClasses += (className -> classBody)

    // return just className, so it will become a type of StructType field,
    // like 'discountInfo: Option[ProductPurchasePayload.DiscountInfo]'
    s"$rootClassNamePlaceholder$className"
  }

  override protected def generateFieldElement(field: StructField, dataTypeElement: String, level: Int): String = {
    val fieldName = escapeIncorrectName(field.name)

    if (field.nullable || makeAllFieldsOptional)
      s"$fieldName: Option[$dataTypeElement] = None"
    else
      s"$fieldName: $dataTypeElement"
  }

  override protected def generateSimpleDataTypeElement(dataType: DataType): String = {
    defaultSimpleTypesConverter(dataType)
  }

  override protected def generateMapTypeElement(keyDataTypeElement: String, valueDataTypeElement: String): String = {
    s"Map[$keyDataTypeElement, $valueDataTypeElement]"
  }

  override protected def generateArrayTypeElement(dataTypeElement: String): String = {
    s"Seq[$dataTypeElement]"
  }

  private def createCaseClassName(fieldName: String): String = {
    val baseClassName = fieldName.split('_')
      .map(_.capitalize)
      .mkString

    if (caseClasses.contains(baseClassName) || usedConventionalClassNames.contains(baseClassName))
      Stream.from(2)
        .map(baseClassName + _)
        .find(cn => !caseClasses.contains(cn)).get
    else
      baseClassName
  }

  private val scalaKeywords = scala.reflect.runtime.universe.asInstanceOf[scala.reflect.internal.SymbolTable].nme.keywords
    .map(_.toString)

  protected def escapeIncorrectName(name: String): String = {
    if (SourceVersion.isKeyword(name))
      name + "___removable_suffix" // Spark can't create DataFrame based on case class with fields named as java keywords
    else if (scalaKeywords.contains(name) || !SourceVersion.isName(name))
      s"`$name`"
    else
      name
  }

  private val usedConventionalClassNames = Seq(
    "Option", "None", "Map", "Seq", "Byte", "Short", "Int", "Long", "Float", "Double", "BigDecimal", "String", "Array",
    "Byte", "Boolean", "Timestamp", "Date"
  )

  private val defaultSimpleTypesConverter: DataType => String =  {
    case _: ByteType => "Byte"
    case _: ShortType => "Short"
    case _: IntegerType => "Int"
    case _: LongType => "Long"
    case _: FloatType => "Float"
    case _: DoubleType => "Double"
    case _: DecimalType => "BigDecimal"
    case _: StringType => "String"
    case _: BinaryType => "Array[Byte]"
    case _: BooleanType => "Boolean"
    case _: TimestampType => "java.sql.Timestamp"
    case _: DateType => "java.sql.Date"
  }
}

object CaseClassesCodeGenerator {

  case class Method(name: String, returnType: String, body: String, isOverride: Boolean = false) {

    def generateCode: String = {
      val ovr = if (isOverride) "override " else ""
      val returnTp = if (returnType.nonEmpty) s": $returnType" else ""
      s"${ovr}def $name$returnTp = $body"
    }
  }
}
