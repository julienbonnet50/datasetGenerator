DatasetGenerator.init(Array(
    /* Principal setting */
    "--datasetGenerator-levelOfNest", "3",
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
    "--datasetGenerator-contains-struct", "true",
    "--datasetGenerator-contains-array", "true",
    "--datasetGenerator-contains-map", "true"
))

DatasetGenerator.main()