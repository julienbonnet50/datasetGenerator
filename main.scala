object DatasetGenerator {

    import java.io
    import org.apache.spark.sql._
    import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._
    
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

    private var simpleType: Seq[String] = null

    private var schema: StructType = null

    def init(args: Array[String]): Unit = {
        @scala.annotation.tailrec
        def nextArg(map: Map[String, Any], list: List[String]): Map[String, Any] = {
            list match {
                case Nil => map
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

        def generateSchema(): Unit = {

        }

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
    }

    def main(): Unit = {
        println("main running")

        val spark = SparkSession
            .builder
            .appName("Dataset Generic Generator")
            .getOrCreate()
    }
}

DatasetGenerator.init(Array(
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
    "--datasetGenerator-contains-varchar", "true",
    "--datasetGenerator-contains-char", "true"
))

DatasetGenerator.main()