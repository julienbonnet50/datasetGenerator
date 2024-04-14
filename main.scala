object DatasetGenerator {

    import java.io
    import org.apache.spark.sql._
    import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.{DataFrame, Row, SparkSession}
    
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
    private var datasetGeneratorLevelOfNest: Integer = 0
    private var datasetGeneratorNumberOfRows: Integer = 1

    private var trueArgs: Seq[String] = Seq.empty

    private var schema: StructType = new StructType()

    def init(args: Array[String]): Unit = {
        @scala.annotation.tailrec
        def nextArg(map: Map[String, Any], list: List[String]): Map[String, Any] = {
            list match {
                case Nil => map
                case "--datasetGenerator-LevelOfNest" :: value :: tail =>
                    nextArg(map ++ Map("datasetGeneratorLevelOfNest" -> value.toInt), tail)
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

        datasetGeneratorLevelOfNest = getOption("datasetGeneratorLevelOfNest", 1).asInstanceOf[Integer]
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
    }

    def generateSchema(fieldNames: Seq[String]): Unit = {
        require(fieldNames.length > 0, "We should at least have 1 field to true")
        for (field <- fieldNames) {
            field match {
                
                /* Simple type */
                case "Byte" => this.schema = this.schema.add(StructField("byte_field", ByteType, true))
                case "Integer" => this.schema = this.schema.add(StructField("int_field", IntegerType, true))
                case "Long" => this.schema = this.schema.add(StructField("long_field", LongType, true))
                case "Short" => this.schema = this.schema.add(StructField("short_field", ShortType, true))
                case "Float" => this.schema = this.schema.add(StructField("float_field", FloatType, true))
                case "Double" => this.schema = this.schema.add(StructField("double_field", DoubleType, true))
                case "Decimal" => this.schema = this.schema.add(StructField("decimal_field", DecimalType(1,1), true))
                case "String" => this.schema = this.schema.add(StructField("string_field", StringType, true))
                case "Varchar" => this.schema = this.schema.add(StructField("varchar_field", VarcharType(10), true))
                case "Char" => this.schema = this.schema.add(StructField("char_field", CharType(10), true))
                case _ => println("Unknown type found " + field)
            }
        }
    }


    def main(): Unit = {
        println("------- Main started running -------")

        generateSchema(trueArgs)

        println(schema)

        val spark = SparkSession
            .builder
            .appName("Dataset Generic Generator")
            .getOrCreate()

        // Creating empty dataFrame with and empty value and right schema.
        val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
        
        df.show()
        df.printSchema
    }
}

DatasetGenerator.init(Array(
    /* Principal setting */
    "--datasetGenerator-LevelOfNest", "2",
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
    "--datasetGenerator-contains-char", "false"
))

DatasetGenerator.main()