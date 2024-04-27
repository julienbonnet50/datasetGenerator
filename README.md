[![Codacy Badge](https://app.codacy.com/project/badge/Grade/831ab1a717f9482e9d71c4dcedb45220)](https://app.codacy.com/gh/julienbonnet50/datasetGenerator/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)

# datasetGenerator

**datasetGenerator** is dataset generic generator with any types and any level of nest made in scala 2.12.

It will firstly generate a schema, depending on type mentionned and level of nest required. Then generate for every field of any types a random value.

## Conf 

You can use /dataset/datasetArgs/conf.scala to set your configurations and just launch the spark-shell after checked all prerequired.

If you are on windows you can just double-click on **spark-shell-3.4.2-launcher.bat**
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
    "--datasetGenerator-NumberOfRows", "100",
    "--datasetGenerator-pathToWrite", "pathTo/generatedDataset",

    /* Numeric type */
    "--datasetGenerator-contains-byte", "true",
    "--datasetGenerator-contains-short", "true",
    "--datasetGenerator-contains-integer", "true",
    "--datasetGenerator-contains-long", "true",
    "--datasetGenerator-contains-float", "true",
    "--datasetGenerator-contains-double", "true",
    "--datasetGenerator-contains-decimal", "false", // Todo

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
Sample of data produced : 

```scala
+-----------+-----------+--------------------+------------+-------------+--------------------+-------------+--------------------+--------------------+--------------------+
|byte_field0| int_field0|         long_field0|short_field0| float_field0|       double_field0|string_field0|       struct_field0|        array_field0|          map_field0|
+-----------+-----------+--------------------+------------+-------------+--------------------+-------------+--------------------+--------------------+--------------------+
|        -35|-2147483648|-1684404113781990136|      -32768|5.0318116E-29|5.279499723346444...|   17e25a70-f|{[{[{{e71d5677-c ...|            [{null}]|{60bd1b57-6 -> {{...|
|       null| 2147483647|-2497803620918967283|           0|         null|                null|   62e1c62d-6|{[{[{{f4692a6a-4 ...|[{{0c4e57a7-c -> ...|{469ae207-2 -> {{...|
|       -128| 1716461154| 2736668824320581175|      -32768|         null|-4.14773011547402...|         null|{[{[{{7827b70a-2 ...|[{{427188ec-2 -> ...|{2ce26f21-a -> {{...|
|          0|-1482187783|                null|       32767|2.1087044E-20|                null|   d0702931-2|{[{[{{8d3c82e8-4 ...|[{{d5639220-7 -> ...|{1bba1cd9-7 -> {{...|
|          0|          0|-9223372036854775808|      -24799|  2.755041E36|-4.25880588504422...|   010493c8-f|{[{[{{befdfa51-1 ...|[{{73db45c0-b -> ...|{78da45c4-f -> {{...|
|        127|-1569212156| 9223372036854775807|      -30478|4.7504805E-18|8.315732226840309...|   a471240a-6|{[{[{{c9187e44-7 ...|[{{6af50bf6-8 -> ...|{0459e50f-a -> {n...|
|         99|          0|                   0|       21323|      1.4E-45|                null|   344da294-7|{[{[{{962ea478-2 ...|              [null]|{39edee5a-1 -> {n...|
|        -92| -248625655| 9223372036854775807|       27751| 3.4028235E38|                null|         null|{[{[{null}, {{575...|            [{null}]|{d231952c-5 -> {n...|
|        -18|          0|                   0|      -32582|      1.4E-45|3.345009149433706...|   2a12d95a-9|{[{[{{878787dc-5 ...|[{{8340958a-0 -> ...|{061b1343-4 -> {{...|
|         46|          0|                null|       23701| 1.4162762E-5|2.510435857565917...|   2998a1e5-f|{[{[{{8ba2d693-2 ...|[{{56672e26-b -> ...|                null|
+-----------+-----------+--------------------+------------+-------------+--------------------+-------------+--------------------+--------------------+--------------------+
```
