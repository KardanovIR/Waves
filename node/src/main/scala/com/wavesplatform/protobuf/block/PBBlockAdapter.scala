package com.wavesplatform.protobuf.block

import com.wavesplatform.block.{BlockHeader, SignerData}
import com.wavesplatform.consensus.nxt.NxtLikeConsensusBlockData
import com.wavesplatform.protobuf.transaction.PBTransactionAdapter
import com.wavesplatform.protobuf.utils.PBImplicitConversions._
import monix.eval.Coeval

import scala.annotation.switch

class PBBlockAdapter(val block: PBCachedBlock)
    extends VanillaBlock(
      block.block.getHeader.timestamp,
      block.block.getHeader.version.toByte,
      block.block.getHeader.reference,
      SignerData(block.block.getHeader.generator.publicKey, block.signature),
      NxtLikeConsensusBlockData(block.block.getHeader.baseTarget, block.block.getHeader.generationSignature),
      block.transactions.map(PBTransactionAdapter(_)),
      block.block.getHeader.featureVotes.iterator.map(_.toShort).toSet
    ) {
  private[this] lazy val vanillaBlock: VanillaBlock = PBBlocks.vanillaUnsafe(block)

  def isLegacy: Boolean = (this.version: @switch) match {
    case 1 | 2 | 3 => true
    case _         => false
  }

  def protoBytes: Array[Byte] = block.bytes

  override val bytes: Coeval[Array[Byte]] =
    if (isLegacy) Coeval.evalOnce(this.protoBytes)
    else vanillaBlock.bytes

  override val bytesWithoutSignature: Coeval[Array[Byte]] = Coeval.evalOnce(
    if (isLegacy) vanillaBlock.bytesWithoutSignature()
    else block.headerBytes // Without txs
  )

  override def getHeader(): BlockHeader =
    new PBBlockAdapter(block.withTransactionsCached(Nil))
}

object PBBlockAdapter {
  def apply(block: PBCachedBlock): PBBlockAdapter = new PBBlockAdapter(block)
  def apply(block: VanillaBlock): PBBlockAdapter = block match {
    case a: PBBlockAdapter => a
    case _                 => new PBBlockAdapter(PBBlocks.protobuf(block))
  }

  def unwrap(block: VanillaBlock): VanillaBlock = block match {
    case a: PBBlockAdapter => PBBlocks.vanillaUnsafe(a.block)
    case _                 => block
  }
}
