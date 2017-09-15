package scorex.testkit.properties.state

import org.scalacheck.Gen
import org.scalatest.prop.{GeneratorDrivenPropertyChecks, PropertyChecks}
import org.scalatest.{Matchers, PropSpec}
import scorex.core.PersistentNodeViewModifier
import scorex.core.transaction.state.MinimalState
import scorex.testkit.TestkitHelpers
import scorex.testkit.generators.{CoreGenerators, SemanticallyInvalidModifierProducer, SemanticallyValidModifierProducer}

trait StateTests[PM <: PersistentNodeViewModifier, ST <: MinimalState[PM, ST]]
  extends PropSpec
    with GeneratorDrivenPropertyChecks
    with Matchers
    with PropertyChecks
    with CoreGenerators
    with TestkitHelpers
    with SemanticallyValidModifierProducer[PM, ST]
    with SemanticallyInvalidModifierProducer[PM, ST] {

  val checksToMake = 10

  val stateGen: Gen[ST]
}
