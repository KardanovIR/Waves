package com.wavesplatform.state

import cats.syntax.semigroup._
import com.wavesplatform.block.Block.BlockId
import com.wavesplatform.block.{Block, MicroBlock}
import com.wavesplatform.common.state.ByteStr

import scala.annotation.tailrec

sealed trait NGS {
  def totalDiff: Diff
  def totalBlock: Block
  def fee: Long
  def carry: Long
  def reward: Option[Long]
  def hitSource: ByteStr
  def timestamp: Long
  def approvedFeatures: Set[Short]

  def forId(id: ByteStr): Option[NGS]

  def append(timestamp: Long, microBlock: MicroBlock, diff: Diff, microBlockCarry: Long, microBlockFee: Long): NGS =
    LiquidBlock(this, timestamp, microBlock, totalDiff |+| diff, this.carry + microBlockCarry, this.fee + microBlockFee)
}

object NGS {
  implicit class NGSExt(val ngs: NGS) extends AnyVal {
    def contains(blockId: ByteStr): Boolean = ngs.forId(blockId).isDefined
    def withExpiredLeases: Diff             = ???
    def microBlockForId(id: ByteStr): Option[MicroBlock] = ngs.forId(id).collect {
      case lb: LiquidBlock => lb.microBlock
    }
    def microBlockIds: Seq[ByteStr] = ???
    def bestLastBlockInfo(maxTimestamp: Long): BlockMinerInfo = {
      @tailrec def findMatchingNGS(startFrom: NGS): NGS = startFrom match {
        case ngs if ngs.timestamp <= maxTimestamp => ngs
        case kb: KeyBlock                         => kb
        case lb: LiquidBlock                      => findMatchingNGS(lb.parent)
      }

      val matchingBlock = findMatchingNGS(ngs).totalBlock
      BlockMinerInfo(matchingBlock.header.baseTarget, matchingBlock.header.generationSignature, matchingBlock.header.timestamp, matchingBlock.id())
    }
  }
}

case class KeyBlock(
    totalBlock: Block,
    totalDiff: Diff,
    carry: Long,
    fee: Long,
    approvedFeatures: Set[Short],
    reward: Option[Long],
    hitSource: ByteStr,
    leasesToCancel: Map[ByteStr, Diff]
) extends NGS {
  override def timestamp: Long                 = totalBlock.header.timestamp
  override def forId(id: BlockId): Option[NGS] = if (totalBlock.id() == id) Some(this) else None
}

case class LiquidBlock(parent: NGS, timestamp: Long, microBlock: MicroBlock, totalDiff: Diff, carry: Long, fee: Long) extends NGS {
  override lazy val reward: Option[Long]       = parent.reward
  override lazy val hitSource: ByteStr         = parent.hitSource
  override lazy val totalBlock: Block          = parent.totalBlock.appendTransactions(microBlock.transactionData, microBlock.totalResBlockSig)
  override def forId(id: BlockId): Option[NGS] = if (totalBlock.id() == id) Some(this) else parent.forId(id)
}
