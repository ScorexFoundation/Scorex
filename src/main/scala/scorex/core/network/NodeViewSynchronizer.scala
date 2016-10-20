package scorex.core.network

import akka.actor.{Actor, ActorRef}
import scorex.core.NodeViewHolder._
import scorex.core.NodeViewModifier.{ModifierId, ModifierTypeId}
import scorex.core.consensus.SyncInfo
import scorex.core.network.NetworkController.{DataFromPeer, SendToNetwork}
import scorex.core.network.message.BasicMsgDataTypes._
import scorex.core.network.message.{InvSpec, RequestModifierSpec, _}
import scorex.core.serialization.ScorexKryoPool
import scorex.core.transaction.Transaction
import scorex.core.transaction.box.proposition.Proposition
import scorex.core.{NodeViewHolder, NodeViewModifier}

import scala.collection.mutable

/**
  * A middle layer between a node view holder(NodeViewHolder) and a network
  *
  * @param networkControllerRef
  * @param viewHolderRef
  * @param syncInfoSpec
  * @tparam P
  * @tparam TX
  * @tparam SIS
  */
class NodeViewSynchronizer[P <: Proposition, TX <: Transaction[P], SI <: SyncInfo, SIS <: SyncInfoSpec[SI]]
(networkControllerRef: ActorRef, viewHolderRef: ActorRef, syncInfoSpec: SIS, pool: ScorexKryoPool) extends Actor {

  import NodeViewSynchronizer._
  import scorex.core.NodeViewModifier._

  //modifier ids asked from other nodes are kept in order to check then
  //against objects sent
  private val asked = mutable.Map[ModifierTypeId, mutable.Set[ModifierId]]()


  private val seniors = mutable.Set[ConnectedPeer]()

  private val juniors = mutable.Set[ConnectedPeer]()

  override def preStart(): Unit = {
    //register as a handler for some types of messages
    val messageSpecs = Seq(InvSpec, RequestModifierSpec, ModifiersSpec, syncInfoSpec)
    networkControllerRef ! NetworkController.RegisterMessagesHandler(messageSpecs, self)

    //subscribe for failed transaction,
    val events = Seq(
      NodeViewHolder.EventType.FailedTransaction,
      NodeViewHolder.EventType.FailedPersistentModifier,
      NodeViewHolder.EventType.SuccessfulTransaction,
      NodeViewHolder.EventType.SuccessfulPersistentModifier,

      NodeViewHolder.EventType.OtherNodeSyncingStatus
    )
    viewHolderRef ! Subscribe(events)
  }

  private def sendModifierIfLocal[M <: NodeViewModifier](m: M, source: Option[ConnectedPeer]): Unit =
    if (source.isEmpty) {
      val data = m.modifierTypeId -> Seq(m.id -> pool.toBytesWithoutClass(m)).toMap
      val msg = Message(ModifiersSpec, Right(data), None)
      networkControllerRef ! SendToNetwork(msg, Broadcast)
    }

  private def viewHolderEvents: Receive = {
    case FailedTransaction(tx, throwable, source) =>
    //todo: ban source peer?
    case FailedModification(mod, throwable, source) =>
    //todo: ban source peer?

    case SuccessfulTransaction(tx, source) => sendModifierIfLocal(tx, source)
    case SuccessfulModification(mod, source) => sendModifierIfLocal(mod, source)
  }


  //sync info is coming from another node
  private def processSync: Receive = {
    case DataFromPeer(spec, syncData: SI, remote)
      if spec.messageCode == syncInfoSpec.messageCode =>

      viewHolderRef ! OtherNodeSyncingInfo(remote, syncData)
  }

  //object ids coming from other node
  private def processInv: Receive = {
    case DataFromPeer(spec, invData: InvData@unchecked, remote)
      if spec.messageCode == InvSpec.messageCode =>

      viewHolderRef ! CompareViews(remote, invData._1, invData._2)
  }

  //other node asking for objects by their ids
  private def modifiersReq: Receive = {
    case DataFromPeer(spec, invData: InvData@unchecked, remote)
      if spec.messageCode == RequestModifierSpec.messageCode =>

      viewHolderRef ! GetLocalObjects(remote, invData._1, invData._2)
  }

  //other node is sending objects
  private def modifiersFromRemote: Receive = {
    case DataFromPeer(spec, data: ModifiersData@unchecked, remote)
      if spec.messageCode == ModifiersSpec.messageCode =>

      val typeId = data._1
      val modifiers = data._2

      val modIds = modifiers.keySet

      val askedIds = asked.getOrElse(typeId, mutable.Set())
      val filteredIds = askedIds.diff(modIds)

      if (askedIds.size - modIds.size == filteredIds.size) {
        val msg = ModifiersFromRemote(remote, data._1, modifiers.valuesIterator.toSeq)
        viewHolderRef ! msg
        asked.put(typeId, filteredIds)
      } else {
        //remote peer has sent some object not requested -> ban!
        //todo: ban a peer
      }
  }

  //local node sending object ids to remote
  private def requestFromLocal: Receive = {
    case RequestFromLocal(peer, modifierTypeId, modifierIds) =>

      if (modifierIds.nonEmpty) {
        val msg = Message(RequestModifierSpec, Right(modifierTypeId -> modifierIds), None)
        peer.handlerRef ! msg
      }
      val newids = asked.getOrElse(modifierTypeId, mutable.Set()) ++ modifierIds
      asked.put(modifierTypeId, newids)
  }

  //local node sending out objects requested to remote
  private def responseFromLocal: Receive = {
    case ResponseFromLocal(peer, typeId, modifiers: Seq[NodeViewModifier]) =>
      if (modifiers.nonEmpty) {
        val modType = modifiers.head.modifierTypeId
        val m = modType -> modifiers.map(m => m.id -> pool.toBytesWithoutClass(m)).toMap
        val msg = Message(ModifiersSpec, Right(m), None)
        peer.handlerRef ! msg
      }
  }

  override def receive: Receive =
    processSync orElse
      processInv orElse
      modifiersReq orElse
      requestFromLocal orElse
      responseFromLocal orElse
      modifiersFromRemote orElse
      viewHolderEvents
}

object NodeViewSynchronizer {

  case class CompareViews(source: ConnectedPeer, modifierTypeId: ModifierTypeId, modifierIds: Seq[ModifierId])

  case class GetLocalObjects(source: ConnectedPeer, modifierTypeId: ModifierTypeId, modifierIds: Seq[ModifierId])

  case class RequestFromLocal(source: ConnectedPeer, modifierTypeId: ModifierTypeId, modifierIds: Seq[ModifierId])

  case class ResponseFromLocal[M <: NodeViewModifier](source: ConnectedPeer, modifierTypeId: ModifierTypeId, localObjects: Seq[M])

  case class ModifiersFromRemote(source: ConnectedPeer, modifierTypeId: ModifierTypeId, remoteObjects: Seq[Array[Byte]])

  case class OtherNodeSyncingInfo[SI <: SyncInfo](peer: ConnectedPeer, syncInfo: SI)

}
