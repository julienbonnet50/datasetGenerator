class SchemaException(message: String) extends Exception(message)

object DatasetGenerator {

    import java.io
    import org.apache.spark.sql._
    import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.{DataFrame, Row, SparkSession}
    
    private var datasetGeneratorlevelOfNest: Integer = 0
    private var datasetGeneratorNumberOfRows: Integer = 1

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

    private var schema: StructType = new StructType()

    

    def init(args: Array[String]): Unit = {
        @scala.annotation.tailrec
        def nextArg(map: Map[String, Any], list: List[String]): Map[String, Any] = {
            list match {
                case Nil => map
                case "--datasetGenerator-levelOfNest" :: value :: tail =>
                    nextArg(map ++ Map("datasetGeneratorlevelOfNest" -> value.toInt), tail)
                case "--datasetGenerator-NumberOfRows" :: value :: tail =>
                    nextArg(map ++ Map("datasetGeneratorNumberOfRows" -> value.toInt), tail)
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

        datasetGeneratorlevelOfNest = getOption("datasetGeneratorlevelOfNest", 1).asInstanceOf[Integer]
        datasetGeneratorNumberOfRows = getOption("datasetGeneratorNumberOfRows", 1).asInstanceOf[Integer]
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
    }

    def generateSchema(fieldNames: Seq[String], levelOfNest: Integer, schemaArg: StructType = new StructType): StructType = {
        require(levelOfNest > 0)
        require(fieldNames.length > 0, "We should at least have 1 field to true")
        if((levelOfNest > 1) && !(fieldNames.contains("Map")) && !(fieldNames.contains("Struct")) && !(fieldNames.contains("Array"))) {
            throw new SchemaException("level of nest : " + levelOfNest + " but any complex type found") 
        }

        def processField(field: String, parentSchema: StructType): StructType = {
            field match {
                /* Simple type */
                case "Byte" => 
                    StructField("byte_field" + levelOfNest, ByteType, true)
                case "Integer" => 
                    StructField("int_field" + levelOfNest, IntegerType, true)
                case "Long" => 
                    StructField("long_field" + levelOfNest, LongType, true)
                case "Short" => 
                    StructField("short_field" + levelOfNest, ShortType, true)
                case "Float" => 
                    StructField("float_field" + levelOfNest, FloatType, true)
                case "Double" => 
                    StructField("double_field" + levelOfNest, DoubleType, true)
                case "Decimal" => 
                    StructField("decimal_field" + levelOfNest, DecimalType(1,1), true)
                case "String" => 
                    StructField("string_field" + levelOfNest, StringType, true)
                case "Varchar" => 
                    StructField("varchar_field" + levelOfNest, VarcharType(10), true)
                case "Char" => 
                    StructField("char_field" + levelOfNest, CharType(10), true)
                // case "Struct" => 
                //     StructField("char_field" + levelOfNest, processField(fieldNames, levelOfNest-1)))
                case _ => 
                    println("Unknown type found " + field)
                    StructField(type + "_field" + levelOfNest, StringType, true)
            }
        }

        for (field <- fieldNames) {

        }
        schemaArg
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

        println("------- App started : " + startTimeApp()  + " -------")

        showArgs()

        println("------- Dataset generation running (took : " + elapsedTime(startTime) + ") -------")

        schema = generateSchema(trueArgs, datasetGeneratorlevelOfNest, schema)

        println(schema)

        // Creating empty dataFrame with and empty value and right schema.
        val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
        
        println("------- Dataset generated show (took : " + elapsedTime(startTime) + ") -------")
        df.show()

        println("------- Schema generated (took : " + elapsedTime(startTime) + ") -------")
        df.printSchema
    }
}

DatasetGenerator.init(Array(
    /* Principal setting */
    "--datasetGenerator-levelOfNest", "1",
    "--datasetGenerator-NumberOfRows", "100",

    /* Numeric type */
    "--datasetGenerator-contains-byte", "true",
    "--datasetGenerator-contains-short", "true",
    "--datasetGenerator-contains-integer", "true",
    "--datasetGenerator-contains-long", "true",
    "--datasetGenerator-contains-float", "true",
    "--datasetGenerator-contains-double", "true",
    "--datasetGenerator-contains-decimal", "true",

    /* String type */
    "--datasetGenerator-contains-string", "true",
    "--datasetGenerator-contains-varchar", "false",
    "--datasetGenerator-contains-char", "false",

    /* Complex type */
    "--datasetGenerator-contains-struct", "false",
    "--datasetGenerator-contains-array", "false",
    "--datasetGenerator-contains-map", "false"
))

DatasetGenerator.main()