/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
// $example off$
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split}


object Word2VecExample {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Word2Vec example")
      .getOrCreate()

    // $example on$
    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    val docDF = spark.read.option("header", "true").csv("/tmp/train.csv")
      .filter(col("question1").isNotNull)
      .select(split(col("question1"), " ").as("text"))
    docDF.printSchema()
    docDF.show()


    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(30)
      .setMaxIter(20)
      .setMinCount(1)
    val model = word2Vec.fit(docDF)

    val result = model.transform(docDF)
    result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
      println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n") }
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
