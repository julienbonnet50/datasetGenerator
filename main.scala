object DatasetGenerator {

    import org.apache.spark.sql._
    import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
    import org.apache.spark.sql.functions._

    def main(args: Array[String]): Unit = {

        @scala.annotation.tailrec
        def nextArg(map: Map[String, Any], list: List[String]): Map[String, Any] = {
            list match {
                case Nil => map
                case "--datasetGenerator-contains-string" :: value :: tail =>
                    nextArg(map ++ Map("" -> value.toBoolean), tail)
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

        val spark = SparkSession
            .builder
            .appName("Dataset Generic Generator")
            .getOrCreate()

        val datasetGeneratorContainsString: Boolean = getOption("datasetGeneratorContainsString", false).asInstanceOf[Boolean]

        println(datasetGeneratorContainsString)
    }
}


DatasetGenerator.main(Array(
    "--datasetGenerator-contains-string", "true"
))