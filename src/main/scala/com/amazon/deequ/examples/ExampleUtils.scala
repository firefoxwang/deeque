/**
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.deequ.examples

import org.apache.spark.sql.{DataFrame, SparkSession}

private[deequ] object ExampleUtils {

  def withSpark(func: SparkSession => Unit): Unit = {
    /*
    高阶函数的一种：函数接收函数 func的类型是函数，输入SparkSession（类变量）类型，输出Unit类型
    Functions that accept functions
    函数（使用=>符号，左边形参parameters,右边是表达式调用an expression involving the parameters）
     */
    val session = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    session.sparkContext.setCheckpointDir(System.getProperty("java.io.tmpdir"))

    try {
      func(session)
      /*
      因为func是函数，因此可以像函数一样使用，入参为session的SparkSession类
      func本身是函数，不是说func的入参为函数，不能func(session=>Unit)
       */

    }

    finally {
      session.stop()
      System.clearProperty("spark.driver.port")
    }
  }

  def itemsAsDataframe(session: SparkSession, items: Item*): DataFrame = {
    /*
    方法的入参在类型后面加*，表示可变参数类型
     */
    val rdd = session.sparkContext.parallelize(items)
    session.createDataFrame(rdd)
  }

  def manufacturersAsDataframe(session: SparkSession, manufacturers: Manufacturer*): DataFrame = {
    val rdd = session.sparkContext.parallelize(manufacturers)
    session.createDataFrame(rdd)
  }
}
