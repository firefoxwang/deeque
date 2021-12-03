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

import ExampleUtils.{withSpark, itemsAsDataframe}
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus

private[examples] object BasicExample extends App {
  /*
  访问修饰符private，protected，public可以通过使用限定词强调,比如private[examples]中的examples,指代某个所属的包、类或单例对象。
  对除了examples之外的所有类都是private的
  注意，是对包examples包及其伴生对象可见，对其他类都不可见。
   */

  withSpark { session =>
    /*
    scala的方法调用详细可以看https://docs.scala-lang.org/style/method-invocation.html
    简单说，scala对于无参数的方法，可以省略圆括号(Scala allows the omission of parentheses on methods of arity-0 (no arguments) )
    对于一个参数的方法，可以使用中缀表示法（不推荐，容易引起争议，只有在operator运算符上才可以，如 1 + 2）
    对于一个参数的高阶函数（Higher-Order Functions，map就是高阶函数）的调用，可以使用parens or braces(就是圆括号或者大括号，这个等会儿深入分析)，高阶函数因为有函数入参，所以尽量使用“.”符号调用方法，
    包在圆括号和大括号的方法，也不能有空格

    关于圆括号或者大括号的分析，见https://stackoverflow.com/questions/4386127/what-is-the-formal-difference-in-scala-between-braces-and-parentheses-and-when?noredirect=1&lq=1
    简单说 在方法调用是多行的情况下，不用圆括号，用花括号
    method {  //建议
      1 +
      2
      3
    }

    method(  // 直接报错
      1 +
      2
      3
    )

    总结上述两篇资料，对于高阶函数，只有一个入参，可以使用花括号（函数需要空一个空格再加花括号）或者圆括号（不加空格），
    如果是多行函数，那么就使用花括号（花括号可以把多行代码（statement）包起来，成为一个block）
     */
    val data = itemsAsDataframe(session,
    /*
    多个入参，只能用大括号
     */
      Item(1, "Thingy A", "awesome thing.", "high", 0),
    // 这边的Item并没有import，原因为import clause is not required for accessing members of the same package.
      // Item和BasicExample在同一个example包下面
      Item(2, "Thingy B", "available at http://thingb.com", null, 0),
      Item(3, null, null, "low", 5),
      Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
      Item(5, "Thingy E", null, "high", 12))

    val verificationResult = VerificationSuite()
      .onData(data)
      .addCheck(
        Check(CheckLevel.Error, "integrity checks")
          // we expect 5 records
          .hasSize(_ == 5)
          // 'id' should never be NULL
          .isComplete("id")
          // 'id' should not contain duplicates
          .isUnique("id")
          // 'productName' should never be NULL
          .isComplete("productName")
          // 'priority' should only contain the values "high" and "low"
          .isContainedIn("priority", Array("high", "low"))
          // 'numViews' should not contain negative values
          .isNonNegative("numViews"))
      .addCheck(
        Check(CheckLevel.Warning, "distribution checks")
          // at least half of the 'description's should contain a url
          .containsURL("description", _ >= 0.5)
          // half of the items should have less than 10 'numViews'
          .hasApproxQuantile("numViews", 0.5, _ <= 10))
      .run()

    if (verificationResult.status == CheckStatus.Success) {
      println("The data passed the test, everything is fine!")
    } else {
      println("We found errors in the data, the following constraints were not satisfied:\n")

      val resultsForAllConstraints = verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }

      resultsForAllConstraints
        .filter { _.status != ConstraintStatus.Success }
        .foreach { result =>
          println(s"${result.constraint} failed: ${result.message.get}")
        }
    }

  }
}
