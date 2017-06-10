package scorex.testkit.properties

import org.scalacheck.Gen
import scorex.core.PersistentNodeViewModifier
import scorex.core.transaction.Transaction
import scorex.core.transaction.box.Box
import scorex.core.transaction.box.proposition.Proposition
import scorex.core.transaction.state.{MinimalState, StateChanges}

import scala.util.Random

trait StateApplyChangesTest[T, P <: Proposition,
TX <: Transaction[P],
PM <: PersistentNodeViewModifier[P, TX],
B <: Box[P, T],
ST <: MinimalState[T, P, B, TX, PM, ST]] extends StateTests[T, P, TX, PM, B, ST] {

  val stateChangesGenerator: Gen[StateChanges[T, P, B]]

  property("State should be able to add a box") {
    forAll(stateChangesGenerator) { c =>
      val newState = state.applyChanges(c, Array.fill(32)(Random.nextInt(Byte.MaxValue).toByte)).get
      c.toAppend.foreach { b =>
        newState.closedBox(b.box.id).isDefined shouldBe true
      }
    }
  }
}
