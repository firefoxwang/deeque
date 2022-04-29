package com.amazon.deequ.examples

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.examples.ExampleUtils.{itemsAsDataframe, withSpark}

object BasicDebugExample extends App {

  withSpark {
    session =>
      val data = itemsAsDataframe(session,
        Item(1, "Thingy A", "awesome thing.", "high", 0),
        Item(2, "Thingy B", "available at http://thingb.com", null, 0),
        Item(3, null, null, "low", 5),
        Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
        Item(5, "Thingy E", null, "high", 12))

      val verificationResult = VerificationSuite()
        .onData(data)
        .addCheck(
          Check(CheckLevel.Error, "integrity checks")
            .containsURL("description", _ >= 0.5)).run()
      if (verificationResult.status == CheckStatus.Success) {
        println("The data passed the test, everything is fine!")
      } else {
        println("We found errors in the data, the following constraints were not satisfied:\n")

        val resultsForAllConstraints = verificationResult.checkResults
          .flatMap { case (_, checkResult) => checkResult.constraintResults }

        resultsForAllConstraints
          .filter {
            _.status != ConstraintStatus.Success
          }
          .foreach { result =>
            println(s"${result.constraint} failed: ${result.message.get}")
          }
      }

  }

}
