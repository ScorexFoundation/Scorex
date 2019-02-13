package scorex.testkit.properties

import akka.actor._
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestProbe}
import akka.util.Timeout
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}
import scorex.core.NodeViewComponent.MempoolComponent
import scorex.core.NodeViewComponentOperation.GetReader
import scorex.core.NodeViewHolder.ReceivableMessages.{GetNodeViewChanges, ModifiersFromRemote}
import scorex.core.PersistentNodeViewModifier
import scorex.core.consensus.History.{Equal, Nonsense, Older, Younger}
import scorex.core.consensus.{History, SyncInfo}
import scorex.core.network.NetworkController.ReceivableMessages.{Blacklist, SendToNetwork}
import scorex.core.network.NetworkControllerSharedMessages.ReceivableMessages.DataFromPeer
import scorex.core.network.NodeViewSynchronizer.Events.{BetterNeighbourAppeared, NoBetterNeighbour, NodeViewSynchronizerEvent}
import scorex.core.network.NodeViewSynchronizer.ReceivableMessages._
import scorex.core.network._
import scorex.core.network.message._
import scorex.core.serialization.{BytesSerializable, Serializer}
import scorex.core.transaction.state.MinimalState
import scorex.core.transaction.{MempoolReader, Transaction}
import scorex.testkit.generators.{SyntacticallyTargetedModifierProducer, TotallyValidModifierProducer}
import scorex.testkit.utils.BaseActorFixture
import scorex.util.ScorexLogging

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

@SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
trait NodeViewSynchronizerTests[TX <: Transaction, PM <: PersistentNodeViewModifier, State <: MinimalState[PM, State],
                                SI <: SyncInfo, HT <: History[PM, SI, HT]]
  extends PropSpec
  with Matchers
  with PropertyChecks
  with ScorexLogging
  with SyntacticallyTargetedModifierProducer[PM, SI, HT]
  with TotallyValidModifierProducer[PM, State, SI, HT] {

  val historyGen: Gen[HT]

  def createFixture(): SynchronizerFixture[TX, PM, SI]

  // ToDo: factor this out of here and NVHTests?
  private def withFixture(testCode: SynchronizerFixture[TX, PM, SI] => Any): Unit = {
    val fixture = createFixture()
    try {
      testCode(fixture)
    }
    finally {
      Await.result(fixture.system.terminate(), Duration.Inf)
    }
  }

  implicit val timeout: Timeout = Timeout(10.seconds)

  property("NodeViewSynchronizer: SuccessfulTransaction") {
    withFixture { ctx =>
      import ctx._
      node ! SuccessfulTransaction[TX](tx)
      ncProbe.fishForMessage(3 seconds) { case m => m.isInstanceOf[SendToNetwork] }
    }
  }

  property("NodeViewSynchronizer: FailedTransaction") {
    withFixture { ctx =>
      import ctx._
      node ! FailedTransaction[TX](tx, new Exception)
      // todo: NVS currently does nothing in this case. Should check banning.
    }
  }

  property("NodeViewSynchronizer: SyntacticallySuccessfulModifier") {
    withFixture { ctx =>
      import ctx._
      node ! SyntacticallySuccessfulModifier(mod)
      // todo ? : NVS currently does nothing in this case. Should it do?
    }
  }

  property("NodeViewSynchronizer: SyntacticallyFailedModification") {
    withFixture { ctx =>
      import ctx._
      node ! SyntacticallyFailedModification(mod, new Exception)
      // todo: NVS currently does nothing in this case. Should check banning.
    }
  }

  property("NodeViewSynchronizer: SemanticallySuccessfulModifier") {
    withFixture { ctx =>
      import ctx._
      node ! SemanticallySuccessfulModifier(mod)
      ncProbe.fishForMessage(3 seconds) { case m => m.isInstanceOf[SendToNetwork] }
    }
  }

  property("NodeViewSynchronizer: SemanticallyFailedModification") {
    withFixture { ctx =>
      import ctx._
      node ! SemanticallyFailedModification(mod, new Exception)
      // todo: NVS currently does nothing in this case. Should check banning.
    }
  }

  //TODO rewrite
  ignore("NodeViewSynchronizer: DataFromPeer: SyncInfoSpec") {
    withFixture { ctx =>
      import ctx._

      val dummySyncInfoMessageSpec = new SyncInfoMessageSpec[SyncInfo](_ => Failure[SyncInfo](new Exception)) {}

      val dummySyncInfo = new SyncInfo {
        def answer: Boolean = true

        def startingPoints: History.ModifierIds = Seq((mod.modifierTypeId, mod.id))

        type M = BytesSerializable

        def serializer: Serializer[M] = throw new Exception
      }

      node ! DataFromPeer(dummySyncInfoMessageSpec, dummySyncInfo, peer)
      //    vhProbe.fishForMessage(3 seconds) { case m => m == OtherNodeSyncingInfo(peer, dummySyncInfo) }
    }
  }

  property("NodeViewSynchronizer: OtherNodeSyncingStatus: Nonsense") {
    withFixture { ctx =>
      import ctx._
      node ! OtherNodeSyncingStatus(peer, Nonsense, None)
      // NVS does nothing in this case
    }
  }

  property("NodeViewSynchronizer: OtherNodeSyncingStatus: Older") {
    withFixture { ctx =>
      import ctx._
      system.eventStream.subscribe(eventListener.ref, classOf[NodeViewSynchronizerEvent])
      node ! OtherNodeSyncingStatus(peer, Older, None)
      eventListener.fishForMessage(3 seconds) { case m => m == BetterNeighbourAppeared }
    }
  }

  property("NodeViewSynchronizer: OtherNodeSyncingStatus: Older and then Younger") {
    withFixture { ctx =>
      import ctx._
      system.eventStream.subscribe(eventListener.ref, classOf[NodeViewSynchronizerEvent])
      node ! OtherNodeSyncingStatus(peer, Older, None)
      node ! OtherNodeSyncingStatus(peer, Younger, None)
      eventListener.fishForMessage(3 seconds) { case m => m == NoBetterNeighbour }
    }
  }

  property("NodeViewSynchronizer: OtherNodeSyncingStatus: Younger with Non-Empty Extension") {
    withFixture { ctx =>
      import ctx._
      node ! OtherNodeSyncingStatus(peer, Younger, Some(Seq((mod.modifierTypeId, mod.id))))
      ncProbe.fishForMessage(3 seconds) { case m =>
        m match {
          case SendToNetwork(Message(_, Right((tid, ids)), None), SendToPeer(p))
            if p == peer && tid == mod.modifierTypeId && ids == Seq(mod.id) => true
          case _ => false
        }
      }
    }
  }

  property("NodeViewSynchronizer: OtherNodeSyncingStatus: Equal") {
    withFixture { ctx =>
      import ctx._
      node ! OtherNodeSyncingStatus(peer, Equal, None)
      // NVS does nothing significant in this case
    }
  }

  property("NodeViewSynchronizer: DataFromPeer: InvSpec") {
    withFixture { ctx =>
      import ctx._
      val spec = new InvSpec(3)
      val modifiers = Seq(mod.id)
      node ! DataFromPeer(spec, (mod.modifierTypeId, modifiers), peer)
      pchProbe.fishForMessage(5 seconds) {
        case _: Message[_] => true
        case _ => false
      }
    }
  }

  property("NodeViewSynchronizer: DataFromPeer: RequestModifierSpec") {
    withFixture { ctx =>
      import ctx._
      @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
      val h = historyGen.sample.get
      val mod = syntacticallyValidModifier(h)
      val (newH, _) = h.append(mod).get
      val spec = new RequestModifierSpec(3)
      val modifiers = Seq(mod.id)
      node ! ChangedHistory(newH)
      node ! DataFromPeer(spec, (mod.modifierTypeId, modifiers), peer)
      (memoryPool ? GetReader(MempoolComponent)).mapTo[MempoolReader[TX]].onComplete {
        case Success(reader) => node ! ChangedMempool(reader)
        case Failure(e) => log.error(s"Cannot get memory pool reader ${e.getMessage}", e)
      }

      pchProbe.fishForMessage(5 seconds) {
        case _: Message[_] => true
        case _ => false
      }
    }
  }

  property("NodeViewSynchronizer: DataFromPeer: Non-Asked Modifiers from Remote") {
    withFixture { ctx =>
      import ctx._

      val modifiersSpec = new ModifiersSpec(1024 * 1024)

      node ! DataFromPeer(modifiersSpec, (mod.modifierTypeId, Map(mod.id -> mod.bytes)), peer)
      val messages = vhProbe.receiveWhile(max = 3 seconds, idle = 1 second) { case m => m }
      assert(!messages.exists(_.isInstanceOf[ModifiersFromRemote[PM]]))
    }
  }

  property("NodeViewSynchronizer: DataFromPeer: Asked Modifiers from Remote") {
    withFixture { ctx =>
      import ctx._
      vhProbe.expectMsgType[GetNodeViewChanges]

      val modifiersSpec = new ModifiersSpec(1024 * 1024)

      node ! DataFromPeer(new InvSpec(3), (mod.modifierTypeId, Seq(mod.id)), peer)
      node ! DataFromPeer(modifiersSpec, (mod.modifierTypeId, Map(mod.id -> mod.bytes)), peer)
      vhProbe.fishForMessage(3 seconds) {
        case m: ModifiersFromRemote[PM] => m.modifiers.toSeq.contains(mod)
        case _ => false
      }
    }
  }

  property("NodeViewSynchronizer: DataFromPeer - CheckDelivery -  Do not penalize if delivered") {
    withFixture { ctx =>
      import ctx._

      val modifiersSpec = new ModifiersSpec(1024 * 1024)

      node ! DataFromPeer(new InvSpec(3), (mod.modifierTypeId, Seq(mod.id)), peer)
      node ! DataFromPeer(modifiersSpec, (mod.modifierTypeId, Map(mod.id -> mod.bytes)), peer)
      system.scheduler.scheduleOnce(1 second, node, DataFromPeer(modifiersSpec, (mod.modifierTypeId, Map(mod.id -> mod.bytes)), peer))
      val messages = ncProbe.receiveWhile(max = 5 seconds, idle = 1 second) { case m => m }
      assert(!messages.contains(Blacklist(peer)))
    }
  }

  property("NodeViewSynchronizer: ResponseFromLocal") {
    withFixture { ctx =>
      import ctx._
      node ! ResponseFromLocal(peer, mod.modifierTypeId, Seq(mod))
      pchProbe.expectMsgType[Message[_]]
    }
  }

}

class SynchronizerFixture[TX <: Transaction, PM <: PersistentNodeViewModifier, SI <: SyncInfo](
    system: ActorSystem,
    val node: ActorRef,
    val memoryPool: ActorRef,
    val syncInfo: SI,
    val mod: PM,
    val tx: TX,
    val peer: ConnectedPeer,
    val pchProbe: TestProbe,
    val ncProbe: TestProbe,
    val vhProbe: TestProbe,
    val eventListener: TestProbe
) extends BaseActorFixture(system)
