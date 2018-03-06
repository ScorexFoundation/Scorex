package examples.trimchain.modifiers

import examples.trimchain.utxo.PersistentAuthenticatedUtxo
import io.circe.Json
import scorex.core.{ModifierId, ModifierTypeId}
import scorex.core.serialization.Serializer
import scorex.core.utils.ByteBoxer

class UtxoSnapshot(override val parentId: ByteBoxer[ModifierId],
                   header: BlockHeader,
                   utxo: PersistentAuthenticatedUtxo) extends TModifier {

  override val modifierTypeId: ModifierTypeId = TModifier.UtxoSnapshot

  //todo: check statically or dynamically output size
  override def id: ModifierId = header.id

  //todo: for Dmitry: implement header + utxo root printing
  override def json: Json = ???

  override type M = UtxoSnapshot

  //todo: for Dmitry: implement: dump all the boxes
  override def serializer: Serializer[M] = ???
}
