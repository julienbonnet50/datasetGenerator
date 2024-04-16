[![Codacy Badge](https://app.codacy.com/project/badge/Grade/831ab1a717f9482e9d71c4dcedb45220)](https://app.codacy.com/gh/julienbonnet50/datasetGenerator/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)

# datasetGenerator

datasetGenerator is dataset generic generator with any types and any level of nest made in scala 2.12.

## Prerequis

Download Spark (currently working with *Spark 3.4.2*)

Download jdk 17.0.8+7

Download hadoop 3.3.0 

Download https://github.com/kontext-tech/winutils/tree/master/hadoop-3.3.0/bin and replace hadoop3.3.0\bin

## Run

Run "spark-shell-3.4.2-launcher.bat" to run a Spark Shell running your main.scala.

## Result

Example of conf with simple type and just one level of nest :
```scala
    /* Principal setting */
    "--datasetGenerator-levelOfNest", "3",

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
```

Result is :

```scala
root
 |-- byte_field0: byte (nullable = true)
 |-- int_field0: integer (nullable = true)
 |-- long_field0: long (nullable = true)
 |-- short_field0: short (nullable = true)
 |-- float_field0: float (nullable = true)
 |-- double_field0: double (nullable = true)
 |-- decimal_field0: decimal(1,1) (nullable = true)
 |-- string_field0: string (nullable = true)
 |-- struct_field0: struct (nullable = true)
 |    |-- struct_field1: struct (nullable = true)
 |    |    |-- array_field2: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- array_field3: array (nullable = true)
 |    |    |    |    |    |-- element: string (containsNull = true)
 |-- array_field0: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- array_field1: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- array_field2: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |-- map_field3: map (nullable = true)
 |    |    |    |    |    |    |    |-- key: string
 |    |    |    |    |    |    |    |-- value: string (valueContainsNull = true)
 |-- map_field0: map (nullable = true)
 |    |-- key: string
 |    |-- value: struct (valueContainsNull = true)
 |    |    |-- map_field1: map (nullable = true)
 |    |    |    |-- key: string
 |    |    |    |-- value: struct (valueContainsNull = true)
 |    |    |    |    |-- map_field2: map (nullable = true)
 |    |    |    |    |    |-- key: string
 |    |    |    |    |    |-- value: struct (valueContainsNull = true)
 |    |    |    |    |    |    |-- map_field3: map (nullable = true)
 |    |    |    |    |    |    |    |-- key: string
 |    |    |    |    |    |    |    |-- value: string (valueContainsNull = true)
```

