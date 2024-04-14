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
```bash
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
    "--datasetGenerator-contains-char", "false"
```

Result is :

```bash
root
 |-- byte_field: byte (nullable = true)
 |-- int_field: integer (nullable = true)
 |-- long_field: long (nullable = true)
 |-- short_field: short (nullable = true)
 |-- float_field: float (nullable = true)
 |-- double_field: double (nullable = true)
 |-- decimal_field: decimal(1,1) (nullable = true)
 |-- string_field: string (nullable = true)
```
with null values
```bash
+----------+---------+----------+-----------+-----------+------------+-------------+------------+
|byte_field|int_field|long_field|short_field|float_field|double_field|decimal_field|string_field|
+----------+---------+----------+-----------+-----------+------------+-------------+------------+
+----------+---------+----------+-----------+-----------+------------+-------------+------------+
```
