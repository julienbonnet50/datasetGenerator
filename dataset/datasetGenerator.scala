import java.util.Random;
import java.math.MathContext

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD

object DatasetGenerator {

    private var datasetGeneratorlevelOfNest: Integer = 0
    private var datasetGeneratorNumberOfRows: Integer = 1
    private var datasetGeneratorPathToWrite: String = "null"
    private var datasetGeneratorRepartitionNum: Integer = 1
    private var datasetGeneratorFilename: String = "null"

    private var datasetGeneratorContainsByte: Boolean = false
    private var datasetGeneratorContainsShort: Boolean = false
    private var datasetGeneratorContainsInteger: Boolean = false
    private var datasetGeneratorContainsLong: Boolean = false
    private var datasetGeneratorContainsFloat: Boolean = false
    private var datasetGeneratorContainsDouble: Boolean = false
    private var datasetGeneratorContainsDecimal: Boolean = false
    private var datasetGeneratorContainsString: Boolean = false
    private var datasetGeneratorContainsVarchar: Boolean = false
    private var datasetGeneratorContainsChar: Boolean = false
    private var datasetGeneratorContainsStruct: Boolean = false
    private var datasetGeneratorContainsArray: Boolean = false
    private var datasetGeneratorContainsMap: Boolean = false
    
    private var trueArgs: Seq[String] = Seq.empty
    private var trueComplexArgs: Seq[String] = Seq.empty

    private var schema: StructType = new StructType()

    val random = new Random


    def init(args: Array[String]): Unit = {
        @scala.annotation.tailrec
        def nextArg(map: Map[String, Any], list: List[String]): Map[String, Any] = {
            list match {
                case Nil => map
                case "--datasetGenerator-levelOfNest" :: value :: tail =>
                    nextArg(map ++ Map("datasetGeneratorlevelOfNest" -> value.toInt), tail)
                case "--datasetGenerator-NumberOfRows" :: value :: tail =>
                    nextArg(map ++ Map("datasetGeneratorNumberOfRows" -> value.toInt), tail)
                case "--datasetGenerator-filename" :: value :: tail =>
                    nextArg(map ++ Map("datasetGeneratorFilename" -> value.toString), tail)
                case "--datasetGenerator-repartitionNum" :: value :: tail =>
                    nextArg(map ++ Map("datasetGeneratorRepartitionNum" -> value.toInt), tail)
                case "--datasetGenerator-pathToWrite" :: value :: tail =>
                    nextArg(map ++ Map("datasetGeneratorPathToWrite" -> value.toString), tail)
                case "--datasetGenerator-contains-byte" :: value :: tail =>
                    nextArg(map ++ Map("datasetGeneratorContainsByte" -> value.toBoolean), tail)
                case "--datasetGenerator-contains-short" :: value :: tail =>
                    nextArg(map ++ Map("datasetGeneratorContainsShort" -> value.toBoolean), tail)
                case "--datasetGenerator-contains-integer" :: value :: tail =>
                    nextArg(map ++ Map("datasetGeneratorContainsInteger" -> value.toBoolean), tail)
                case "--datasetGenerator-contains-long" :: value :: tail =>
                    nextArg(map ++ Map("datasetGeneratorContainsLong" -> value.toBoolean), tail)
                case "--datasetGenerator-contains-float" :: value :: tail =>
                    nextArg(map ++ Map("datasetGeneratorContainsFloat" -> value.toBoolean), tail)
                case "--datasetGenerator-contains-double" :: value :: tail =>
                    nextArg(map ++ Map("datasetGeneratorContainsDouble" -> value.toBoolean), tail)
                case "--datasetGenerator-contains-decimal" :: value :: tail =>
                    nextArg(map ++ Map("datasetGeneratorContainsDecimal" -> value.toBoolean), tail)
                case "--datasetGenerator-contains-string" :: value :: tail =>
                    nextArg(map ++ Map("datasetGeneratorContainsString" -> value.toBoolean), tail)
                case "--datasetGenerator-contains-varchar" :: value :: tail =>
                    nextArg(map ++ Map("datasetGeneratorContainsVarchar" -> value.toBoolean), tail)
                case "--datasetGenerator-contains-char" :: value :: tail =>
                    nextArg(map ++ Map("datasetGeneratorContainsChar" -> value.toBoolean), tail)
                case "--datasetGenerator-contains-struct" :: value :: tail =>
                    nextArg(map ++ Map("datasetGeneratorContainsStruct" -> value.toBoolean), tail)
                case "--datasetGenerator-contains-array" :: value :: tail =>
                    nextArg(map ++ Map("datasetGeneratorContainsArray" -> value.toBoolean), tail)
                case "--datasetGenerator-contains-map" :: value :: tail =>
                    nextArg(map ++ Map("datasetGeneratorContainsMap" -> value.toBoolean), tail)
                case "--" :: _ =>
                    map
                case unknown :: _ =>
                    // TODO : Replace println with logger
                    println("Unknown option : " + unknown)
                    scala.sys.exit(1)
            }
        }
        
        val options = nextArg(Map(), args.toList)

        def getOption(name: String, defaultValue: Any = null): Any = {
            try {
                options(name)
            } catch {
                case _ : Throwable =>
                    if (defaultValue != null)
                        defaultValue
                    else scala.sys.error(s"No option " + name + " found in command line")
            }
        }

        datasetGeneratorRepartitionNum = getOption("datasetGeneratorRepartitionNum", 1).asInstanceOf[Integer]
        datasetGeneratorlevelOfNest = getOption("datasetGeneratorlevelOfNest", 1).asInstanceOf[Integer]
        datasetGeneratorNumberOfRows = getOption("datasetGeneratorNumberOfRows", 1).asInstanceOf[Integer]
        datasetGeneratorPathToWrite = getOption("datasetGeneratorPathToWrite", "pathTo/generatedDataset").asInstanceOf[String]
        datasetGeneratorFilename = getOption("datasetGeneratorFilename", "datasetGenerated").asInstanceOf[String]

        datasetGeneratorContainsByte = getOption("datasetGeneratorContainsByte", false).asInstanceOf[Boolean]
        datasetGeneratorContainsShort = getOption("datasetGeneratorContainsShort", false).asInstanceOf[Boolean]
        datasetGeneratorContainsInteger = getOption("datasetGeneratorContainsInteger", false).asInstanceOf[Boolean]
        datasetGeneratorContainsLong = getOption("datasetGeneratorContainsLong", false).asInstanceOf[Boolean]
        datasetGeneratorContainsFloat = getOption("datasetGeneratorContainsFloat", false).asInstanceOf[Boolean]
        datasetGeneratorContainsDouble = getOption("datasetGeneratorContainsDouble", false).asInstanceOf[Boolean]
        datasetGeneratorContainsDecimal = getOption("datasetGeneratorContainsDecimal", false).asInstanceOf[Boolean]
        datasetGeneratorContainsString = getOption("datasetGeneratorContainsString", false).asInstanceOf[Boolean]
        datasetGeneratorContainsVarchar = getOption("datasetGeneratorContainsVarchar", false).asInstanceOf[Boolean]
        datasetGeneratorContainsChar = getOption("datasetGeneratorContainsChar", false).asInstanceOf[Boolean]
        datasetGeneratorContainsStruct = getOption("datasetGeneratorContainsStruct", false).asInstanceOf[Boolean]
        datasetGeneratorContainsArray = getOption("datasetGeneratorContainsArray", false).asInstanceOf[Boolean]
        datasetGeneratorContainsMap = getOption("datasetGeneratorContainsMap", false).asInstanceOf[Boolean]

        if (datasetGeneratorContainsByte == true) trueArgs :+= "Byte"
        if (datasetGeneratorContainsInteger == true) trueArgs :+= "Integer"
        if (datasetGeneratorContainsLong == true) trueArgs :+= "Long"
        if (datasetGeneratorContainsShort == true) trueArgs :+= "Short"
        if (datasetGeneratorContainsFloat == true) trueArgs :+= "Float"
        if (datasetGeneratorContainsDouble == true) trueArgs :+= "Double"
        if (datasetGeneratorContainsDecimal == true) trueArgs :+= "Decimal"
        if (datasetGeneratorContainsString == true) trueArgs :+= "String"
        if (datasetGeneratorContainsVarchar == true) trueArgs :+= "Varchar"
        if (datasetGeneratorContainsChar == true) trueArgs :+= "Char"
        if (datasetGeneratorContainsStruct == true) trueArgs :+= "Struct"
        if (datasetGeneratorContainsArray == true) trueArgs :+= "Array"
        if (datasetGeneratorContainsMap == true) trueArgs :+= "Map"

        if (datasetGeneratorContainsStruct == true) trueComplexArgs :+= "Struct"
        if (datasetGeneratorContainsArray == true) trueComplexArgs :+= "Array"
        if (datasetGeneratorContainsMap == true) trueComplexArgs :+= "Map"
    }

    def generateSchema(fieldNames: Seq[String], levelOfNestMax: Integer): StructType = {
        require(levelOfNestMax > 0)
        require(fieldNames.length > 0, "We should at least have 1 field to true")
        if((levelOfNestMax > 1) && !(fieldNames.contains("Map")) && !(fieldNames.contains("Struct")) && !(fieldNames.contains("Array"))) {
            throw new SchemaException("level of nest : " + levelOfNestMax + " but any complex type found") 
        }

        def pickRandomValue(values: Seq[String]): String = {

            if (values.isEmpty) {
                throw new IllegalArgumentException("Sequence cannot be empty")
            }
            val randomIndex = this.random.nextInt(values.length)
            values(randomIndex)
        }

        def getDataType(typeName: String): DataType = {
            typeName.toLowerCase match {
                case "Array" => ArrayType(StringType)
                case "Struct" => StructType(Seq.empty)
                case "Map" => MapType(StringType, StringType)
                case _ => StringType
            }
        }

        def processField(field: String, parentSchema: StructType, levelOfNestProcessed: Integer): StructType = {
            field match {
                /* Simple type */
                case "Byte" => 
                    parentSchema.add(StructField("byte_field" + levelOfNestProcessed, ByteType, true))
                case "Integer" => 
                    parentSchema.add(StructField("int_field" + levelOfNestProcessed, IntegerType, true))
                case "Long" => 
                    parentSchema.add(StructField("long_field" + levelOfNestProcessed, LongType, true))
                case "Short" => 
                    parentSchema.add(StructField("short_field" + levelOfNestProcessed, ShortType, true))
                case "Float" => 
                    parentSchema.add(StructField("float_field" + levelOfNestProcessed, FloatType, true))
                case "Double" => 
                    parentSchema.add(StructField("double_field" + levelOfNestProcessed, DoubleType, true))
                case "Decimal" => 
                    parentSchema.add(StructField("decimal_field" + levelOfNestProcessed, DecimalType(1,1), true))
                case "String" => 
                    parentSchema.add(StructField("string_field" + levelOfNestProcessed, StringType, true))
                case "Varchar" => 
                    parentSchema.add(StructField("varchar_field" + levelOfNestProcessed, VarcharType(10), true))
                case "Char" => 
                    parentSchema.add(StructField("char_field" + levelOfNestProcessed, CharType(10), true))
                case "Struct" => 
                    val nestedSchema: StructType = new StructType
                    if (levelOfNestProcessed < levelOfNestMax) {
                        val randomComplexType = pickRandomValue(this.trueComplexArgs)
                        nestedSchema.add(StructField(randomComplexType + "_field" + levelOfNestProcessed, getDataType(randomComplexType), true))
                        parentSchema.add(StructField("struct_field" + levelOfNestProcessed, processField(randomComplexType, nestedSchema, levelOfNestProcessed + 1)))
                    } else {
                        nestedSchema.add(StructField("string_field" + levelOfNestProcessed, StringType, true))
                        parentSchema.add(StructField("struct_field" + levelOfNestProcessed, processField("String", nestedSchema, levelOfNestProcessed + 1)))
                    }
                case "Array" => 
                    if (levelOfNestProcessed < levelOfNestMax) {
                        val nestedSchema: StructType = new StructType
                        val randomComplexType = pickRandomValue(this.trueComplexArgs)
                        nestedSchema.add(StructField(randomComplexType + "_field" + levelOfNestProcessed, getDataType(randomComplexType), true))
                        parentSchema.add(StructField("array_field" + levelOfNestProcessed, ArrayType(processField(randomComplexType, nestedSchema, levelOfNestProcessed + 1))))
                    } else {
                        parentSchema.add(StructField("array_field" + levelOfNestProcessed, ArrayType(StringType), true))
                    }
                case "Map" => 
                if (levelOfNestProcessed < levelOfNestMax + 1) { // Map as a sublevel of key/value 
                    val nestedSchema: StructType = new StructType
                    val randomComplexType = pickRandomValue(this.trueComplexArgs)
                    nestedSchema.add(StructField(randomComplexType + "_field" + levelOfNestProcessed, getDataType(randomComplexType), true))
                    parentSchema.add(StructField("map_field" + levelOfNestProcessed, MapType(getDataType(randomComplexType), processField(randomComplexType, nestedSchema, levelOfNestProcessed + 1))))
                } else {
                    parentSchema.add(StructField("map_field" + levelOfNestProcessed, MapType(StringType, StringType)))
                }
                case _ => 
                    println("Unknown type found " + field)
                    parentSchema
            }
        }

        var schemaGenerated = new StructType()

        for (field <- fieldNames) {
            schemaGenerated = processField(field, schemaGenerated, 0)
        }
        schemaGenerated
    }

    def elapsedTime(startTime: Long): Double = {
        val endTime = System.nanoTime()
        val elapsedTime = (endTime - startTime) / 1e9d
        elapsedTime
    }
    
    def startTimeApp(): String = {
        import java.time.LocalDateTime
        import java.time.format.DateTimeFormatter

        val currentDateTime = LocalDateTime.now()
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val formattedDateTime = currentDateTime.format(formatter)
        formattedDateTime.toString
    } 

    def showArgs(): Unit = {
        println("level of nest : " + datasetGeneratorlevelOfNest)
        println("number of rows : " + datasetGeneratorNumberOfRows)
        println("true type arguments : " + trueArgs)
    } 
        
     
    def main(): Unit = {
        val startTime = System.nanoTime()

        println("\n ------- App started : " + startTimeApp()  + " ------- \n")

        showArgs()

        println("\n ------- Schema generation started running ------- \n")

        schema = generateSchema(trueArgs, datasetGeneratorlevelOfNest - 1)

        println(schema)

        println("\n ------- Schema generation ended (took : " + elapsedTime(startTime) + ") ------- \n")

        println("\n ------- Dataset generation starting (took : " + elapsedTime(startTime) + ") ------- \n")

        val rows = (1 to 10).map(_ => RandomDataGenerator.randomRow(this.random, schema))

        val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
        
        println("\n ------- Dataset generated show (took : " + elapsedTime(startTime) + ") ------- \n")
        
        df.show()

        println("\n ------- Schema generated (took : " + elapsedTime(startTime) + ") ------- \n")

        df.printSchema

        println("\n ------- Saving dataset into " + datasetGeneratorPathToWrite + "/" + datasetGeneratorFilename + " ------- \n")

        val dfDone = df
            .repartition(datasetGeneratorRepartitionNum)

        dfDone
            .write
            .format("parquet")
            .mode("overwrite")
            .save(datasetGeneratorPathToWrite + "/" + datasetGeneratorFilename)
    }
}