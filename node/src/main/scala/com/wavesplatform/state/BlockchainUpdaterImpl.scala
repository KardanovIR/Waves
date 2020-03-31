package com.wavesplatform.state

import java.util.concurrent.locks.{Lock, ReentrantReadWriteLock}

import cats.implicits._
import cats.kernel.Monoid
import com.wavesplatform.account.Address
import com.wavesplatform.api.BlockMeta
import com.wavesplatform.block.Block.BlockId
import com.wavesplatform.block.{Block, MicroBlock}
import com.wavesplatform.common.state.ByteStr
import com.wavesplatform.database.LevelDBWriter
import com.wavesplatform.events.BlockchainUpdateTriggers
import com.wavesplatform.features.BlockchainFeatures
import com.wavesplatform.features.FeatureProvider._
import com.wavesplatform.lang.ValidationError
import com.wavesplatform.metrics.{TxsInBlockchainStats, _}
import com.wavesplatform.mining.{MiningConstraint, MiningConstraints}
import com.wavesplatform.settings.WavesSettings
import com.wavesplatform.state.diffs.BlockDiffer
import com.wavesplatform.state.reader.CompositeBlockchain
import com.wavesplatform.transaction.TxValidationError.{BlockAppendError, GenericError, MicroBlockAppendError}
import com.wavesplatform.transaction._
import com.wavesplatform.transaction.lease._
import com.wavesplatform.utils.{ScorexLogging, Time, UnsupportedFeature, forceStopApplication}
import kamon.Kamon
import monix.reactive.subjects.ReplaySubject
import monix.reactive.{Observable, Observer}

class BlockchainUpdaterImpl(
    private val leveldb: LevelDBWriter,
    spendableBalanceChanged: Observer[(Address, Asset)],
    wavesSettings: WavesSettings,
    time: Time,
    blockchainUpdateTriggers: BlockchainUpdateTriggers
) extends BlockchainUpdater
    with NG
    with ScorexLogging {

  import com.wavesplatform.state.BlockchainUpdaterImpl._
  import wavesSettings.blockchainSettings.functionalitySettings

  private def inLock[R](l: Lock, f: => R): R = {
    l.lockInterruptibly()
    try f
    finally l.unlock()
  }

  private val lock                     = new ReentrantReadWriteLock(true)
  private def writeLock[B](f: => B): B = inLock(lock.writeLock(), f)

  private lazy val maxBlockReadinessAge = wavesSettings.minerSettings.intervalAfterLastBlockThenGenerationIsAllowed.toMillis

  private var ngState: Option[NGS]                  = Option.empty
  private var restTotalConstraint: MiningConstraint = MiningConstraints(leveldb, leveldb.height).total

  private val internalLastBlockInfo = ReplaySubject.createLimited[LastBlockInfo](1)

  private def lastBlockReward: Option[Long] = blockchain.blockReward(blockchain.height)

  private def publishLastBlockInfo(): Unit =
    for (header <- blockchain.lastBlockHeader) {
      val blockchainReady = header.header.timestamp + maxBlockReadinessAge > time.correctedTime()
      internalLastBlockInfo.onNext(LastBlockInfo(header.id(), blockchain.height, blockchain.score, blockchainReady))
    }

  publishLastBlockInfo()

  def liquidBlock(id: ByteStr): Option[Block] = ngState.flatMap(_.forId(id).map(_.totalBlock))

  def liquidBlockMeta: Option[BlockMeta] =
    ngState.map { ng =>
      val b   = ng.totalBlock
      val vrf = if (b.header.version >= Block.ProtoBlockVersion) blockchain.hitSource(blockchain.height) else None
      BlockMeta.fromBlock(b, blockchain.height, ng.fee, ng.reward, vrf)
    }

  def bestLiquidDiff: Option[Diff] = ngState.map(_.totalDiff)

  def blockchain: Blockchain = ngState.fold[Blockchain](leveldb)(CompositeBlockchain(leveldb, _))

  override def isLastBlockId(id: ByteStr): Boolean =
    ngState.fold(leveldb.lastBlockId.contains(id))(_.contains(id))

  override val lastBlockInfo: Observable[LastBlockInfo] = internalLastBlockInfo

  private def featuresApprovedWithBlock(block: Block): Set[Short] = {
    val height = leveldb.height + 1

    val featuresCheckPeriod        = functionalitySettings.activationWindowSize(height)
    val blocksForFeatureActivation = functionalitySettings.blocksForFeatureActivation(height)

    if (height % featuresCheckPeriod == 0) {
      val approvedFeatures = leveldb.featureVotes
        .map { case (feature, votes) => feature -> (if (block.header.featureVotes.contains(feature)) votes + 1 else votes) }
        .filter { case (_, votes) => votes >= blocksForFeatureActivation }
        .keySet

      if (approvedFeatures.nonEmpty) log.info(s"${displayFeatures(approvedFeatures)} APPROVED at height $height")

      val unimplementedApproved = approvedFeatures.diff(BlockchainFeatures.implemented)
      if (unimplementedApproved.nonEmpty) {
        log.warn(s"""UNIMPLEMENTED ${displayFeatures(unimplementedApproved)} APPROVED ON BLOCKCHAIN
                    |PLEASE, UPDATE THE NODE AS SOON AS POSSIBLE
                    |OTHERWISE THE NODE WILL BE STOPPED OR FORKED UPON FEATURE ACTIVATION""".stripMargin)
      }

      val activatedFeatures: Set[Short] = leveldb.activatedFeaturesAt(height)

      val unimplementedActivated = activatedFeatures.diff(BlockchainFeatures.implemented)
      if (unimplementedActivated.nonEmpty) {
        log.error(s"UNIMPLEMENTED ${displayFeatures(unimplementedActivated)} ACTIVATED ON BLOCKCHAIN")
        log.error("PLEASE, UPDATE THE NODE IMMEDIATELY")
        if (wavesSettings.featuresSettings.autoShutdownOnUnsupportedFeature) {
          log.error("FOR THIS REASON THE NODE WAS STOPPED AUTOMATICALLY")
          forceStopApplication(UnsupportedFeature)
        } else log.error("OTHERWISE THE NODE WILL END UP ON A FORK")
      }

      approvedFeatures
    } else {

      Set.empty
    }
  }

  private def nextReward(): Option[Long] = {
    val settings   = blockchain.settings.rewardsSettings
    val nextHeight = blockchain.height + 1

    leveldb
      .featureActivationHeight(BlockchainFeatures.BlockReward.id)
      .filter(_ <= nextHeight)
      .flatMap { activatedAt =>
        val mayBeReward     = lastBlockReward
        val mayBeTimeToVote = nextHeight - activatedAt

        mayBeReward match {
          case Some(reward) if mayBeTimeToVote > 0 && mayBeTimeToVote % settings.term == 0 =>
            Some((blockchain.blockRewardVotes.filter(_ >= 0), reward))
          case None if mayBeTimeToVote >= 0 =>
            Some((Seq(), settings.initial))
          case _ => None
        }
      }
      .flatMap {
        case (votes, currentReward) =>
          val lt        = votes.count(_ < currentReward)
          val gt        = votes.count(_ > currentReward)
          val threshold = settings.votingInterval / 2 + 1

          if (lt >= threshold)
            Some(math.max(currentReward - settings.minIncrement, 0))
          else if (gt >= threshold)
            Some(currentReward + settings.minIncrement)
          else
            Some(currentReward)
      }
      .orElse(lastBlockReward)
  }

  override def processBlock(block: Block, hitSource: ByteStr, verify: Boolean = true): Either[ValidationError, Option[DiscardedTransactions]] =
    writeLock {
      val height                             = leveldb.height
      val notImplementedFeatures: Set[Short] = leveldb.activatedFeaturesAt(height).diff(BlockchainFeatures.implemented)

      Either
        .cond(
          !wavesSettings.featuresSettings.autoShutdownOnUnsupportedFeature || notImplementedFeatures.isEmpty,
          (),
          GenericError(s"UNIMPLEMENTED ${displayFeatures(notImplementedFeatures)} ACTIVATED ON BLOCKCHAIN, UPDATE THE NODE IMMEDIATELY")
        )
        .flatMap[ValidationError, Option[DiscardedTransactions]](
          _ =>
            (ngState match {
              case None =>
                leveldb.lastBlockId match {
                  case Some(uniqueId) if uniqueId != block.header.reference =>
                    val logDetails = s"The referenced block(${block.header.reference})" +
                      s" ${if (leveldb.contains(block.header.reference)) "exits, it's not last persisted" else "doesn't exist"}"
                    Left(BlockAppendError(s"References incorrect or non-existing block: " + logDetails, block))
                  case lastBlockId =>
                    val height            = lastBlockId.fold(0)(leveldb.unsafeHeightOf)
                    val miningConstraints = MiningConstraints(leveldb, height)
                    val reward            = nextReward()
                    BlockDiffer
                      .fromBlock(
                        CompositeBlockchain(leveldb, reward = reward),
                        leveldb.lastBlock,
                        block,
                        miningConstraints.total,
                        verify
                      )
                      .map(r => Option((r, Seq.empty[Transaction], reward, hitSource)))
                }
              case Some(ng) =>
                if (ng.totalBlock.header.reference == block.header.reference) {
                  if (block.blockScore() > ng.totalBlock.blockScore()) {
                    val height            = leveldb.unsafeHeightOf(ng.totalBlock.header.reference)
                    val miningConstraints = MiningConstraints(leveldb, height)

                    blockchainUpdateTriggers.onMicroBlockRollback(block.header.reference, blockchain.height)

                    BlockDiffer
                      .fromBlock(
                        CompositeBlockchain(leveldb, reward = ng.reward),
                        leveldb.lastBlock,
                        block,
                        miningConstraints.total,
                        verify
                      )
                      .map { r =>
                        log.trace(
                          s"Better liquid block(score=${block.blockScore()}) received and applied instead of existing(score=${ng.totalBlock.blockScore()})"
                        )
                        Some((r, ng.totalBlock.transactionData, ng.reward, hitSource))
                      }
                  } else if (areVersionsOfSameBlock(block, ng.totalBlock)) {
                    if (block.transactionData.lengthCompare(ng.totalBlock.transactionData.size) <= 0) {
                      log.trace(s"Existing liquid block is better than new one, discarding $block")
                      Right(None)
                    } else {
                      log.trace(s"New liquid block is better version of existing, swapping")
                      val height            = leveldb.unsafeHeightOf(ng.totalBlock.header.reference)
                      val miningConstraints = MiningConstraints(leveldb, height)

                      blockchainUpdateTriggers.onMicroBlockRollback(block.header.reference, blockchain.height)

                      BlockDiffer
                        .fromBlock(
                          CompositeBlockchain(leveldb, reward = ng.reward),
                          leveldb.lastBlock,
                          block,
                          miningConstraints.total,
                          verify
                        )
                        .map(r => Some((r, Seq.empty[Transaction], ng.reward, hitSource)))
                    }
                  } else
                    Left(
                      BlockAppendError(
                        s"Competitors liquid block $block(score=${block.blockScore()}) " +
                          s"is not better than existing (ng.base ${ng.totalBlock}(score=${ng.totalBlock.blockScore()}))",
                        block
                      )
                    )
                } else
                  metrics.forgeBlockTimeStats.measureSuccessful(ng.forId(block.header.reference)) match {
                    case None => Left(BlockAppendError(s"References incorrect or non-existing block", block))
                    case Some(ngs) =>
                      if (!verify || ngs.totalBlock.signatureValid()) {
                        val height = leveldb.heightOf(ngs.totalBlock.header.reference).getOrElse(0)

                        val discarded: Seq[MicroBlock] = ???

                        if (discarded.nonEmpty) {
                          blockchainUpdateTriggers.onMicroBlockRollback(ngs.totalBlock.id(), blockchain.height)
                          metrics.microBlockForkStats.increment()
                          metrics.microBlockForkHeightStats.record(discarded.size)
                        }

                        val constraint = MiningConstraints(leveldb, height).total

                        val prevReward = ng.reward
                        val reward     = nextReward()

                        val prevHitSource = ng.hitSource

                        val liquidDiffWithCancelledLeases = ngs.withExpiredLeases

                        val diff = BlockDiffer
                          .fromBlock(
                            blockchain,
                            Some(ngs.totalBlock),
                            block,
                            constraint,
                            verify
                          )

                        diff.map { hardenedDiff =>
                          leveldb.append(liquidDiffWithCancelledLeases, ngs.carry, ngs.fee, prevReward, prevHitSource, ngs.totalBlock)
                          BlockStats.appended(ngs.totalBlock, ngs.totalDiff.scriptsComplexity)
                          TxsInBlockchainStats.record(ngs.totalBlock.transactionData.size)
                          Some((hardenedDiff, discarded.flatMap(_.transactionData), reward, hitSource))
                        }
                      } else {
                        val errorText = "Forged block has invalid signature"
                        log.error(errorText)
                        Left(BlockAppendError(errorText, block))
                      }
                  }
            }).map {
              _ map {
                case (BlockDiffer.Result(newBlockDiff, carry, totalFee, updatedTotalConstraint, detailedDiff), discarded, reward, hitSource) =>
                  val newHeight   = leveldb.height + 1
                  val prevNgState = ngState

                  restTotalConstraint = updatedTotalConstraint
                  ngState = Some(
                    KeyBlock(
                      block,
                      newBlockDiff,
                      carry,
                      totalFee,
                      featuresApprovedWithBlock(block),
                      reward,
                      hitSource,
                      cancelLeases(collectLeasesToCancel(newHeight))
                    )
                  )
                  notifyChangedSpendable(prevNgState, ngState)
                  publishLastBlockInfo()

                  if ((block.header.timestamp > time
                        .getTimestamp() - wavesSettings.minerSettings.intervalAfterLastBlockThenGenerationIsAllowed.toMillis) || (newHeight % 100 == 0)) {
                    log.info(s"New height: $newHeight")
                  }

                  blockchainUpdateTriggers.onProcessBlock(block, detailedDiff, reward, leveldb)

                  discarded
              }
            }
        )
    }

  private def collectLeasesToCancel(newHeight: Int): Seq[LeaseTransaction] =
    if (leveldb.isFeatureActivated(BlockchainFeatures.LeaseExpiration, newHeight)) {
      val toHeight = newHeight - leveldb.settings.functionalitySettings.leaseExpiration
      val fromHeight = leveldb.featureActivationHeight(BlockchainFeatures.LeaseExpiration.id) match {
        case Some(activationHeight) if activationHeight == newHeight => 1
        case _                                                       => toHeight
      }
      log.trace(s"Collecting leases created within [$fromHeight, $toHeight]")
      leveldb.collectActiveLeases(_ => true)
    } else Seq.empty

  private def cancelLeases(leaseTransactions: Seq[LeaseTransaction]): Map[ByteStr, Diff] =
    (for {
      lt        <- leaseTransactions
      recipient <- leveldb.resolveAlias(lt.recipient).toSeq
    } yield lt.id() -> Diff.empty.copy(
      portfolios = Map(
        lt.sender.toAddress -> Portfolio(0, LeaseBalance(0, -lt.amount), Map.empty),
        recipient           -> Portfolio(0, LeaseBalance(-lt.amount, 0), Map.empty)
      ),
      leaseState = Map(lt.id() -> false)
    )).toMap

  override def removeAfter(blockId: ByteStr): Either[ValidationError, Seq[(Block, ByteStr)]] = writeLock {
    log.info(s"Removing blocks after ${blockId.trim} from blockchain")

    val prevNgState = ngState
    val result = if (prevNgState.exists(_.contains(blockId))) {
      log.trace("Resetting liquid block, no rollback is necessary")
      blockchainUpdateTriggers.onMicroBlockRollback(blockId, blockchain.height)
      Right(Seq.empty)
    } else {
      val discardedNgBlock = prevNgState.map(ng => (ng.totalBlock, ng.hitSource)).toSeq
      ngState = None
      leveldb
        .rollbackTo(blockId)
        .map { bs =>
          blockchainUpdateTriggers.onRollback(blockId, leveldb.height)
          bs ++ discardedNgBlock
        }
        .leftMap(err => GenericError(err))
    }

    notifyChangedSpendable(prevNgState, ngState)
    publishLastBlockInfo()
    result
  }

  private def notifyChangedSpendable(prevNgState: Option[NGS], newNgState: Option[NGS]): Unit = {
    val changedPortfolios = (prevNgState, newNgState) match {
      case (Some(p), Some(n)) => diff(p.totalDiff.portfolios, n.totalDiff.portfolios)
      case (Some(x), _)       => x.totalDiff.portfolios
      case (_, Some(x))       => x.totalDiff.portfolios
      case _                  => Map.empty
    }

    changedPortfolios.foreach {
      case (addr, p) =>
        p.assetIds.view
          .filter(x => p.spendableBalanceOf(x) != 0)
          .foreach(assetId => spendableBalanceChanged.onNext(addr -> assetId))
    }
  }

  override def processMicroBlock(microBlock: MicroBlock, verify: Boolean = true): Either[ValidationError, BlockId] =
    ngState match {
      case None =>
        Left(MicroBlockAppendError("No base block exists", microBlock))
      case Some(ng) if ng.totalBlock.header.generator.toAddress != microBlock.sender.toAddress =>
        Left(MicroBlockAppendError("Base block has been generated by another account", microBlock))
      case Some(ng) if ng.totalBlock.id() != microBlock.reference =>
        metrics.microMicroForkStats.increment()
        Left(MicroBlockAppendError("It doesn't reference last known microBlock(which exists)", microBlock))
      case Some(ng) =>
        for {
          _ <- microBlock.signaturesValid()
          totalSignatureValid <- ng
            .forId(microBlock.reference)
            .toRight(GenericError(s"No referenced block exists: $microBlock"))
            .map { ngs =>
              ngs.totalBlock
                .appendTransactions(microBlock.transactionData, microBlock.totalResBlockSig)
                .signatureValid()
            }
          _ <- Either
            .cond(
              totalSignatureValid,
              Unit,
              MicroBlockAppendError("Invalid total block signature", microBlock)
            )
          blockDifferResult <- BlockDiffer.fromMicroBlock(
            blockchain,
            leveldb.lastBlockTimestamp,
            microBlock,
            ng.totalBlock.header.timestamp,
            restTotalConstraint,
            verify
          )

        } yield {
          val BlockDiffer.Result(diff, carry, totalFee, updatedMdConstraint, detailedDiff) = blockDifferResult
          restTotalConstraint = updatedMdConstraint
          val blockId: ByteStr = ??? //ng.createBlockId(microBlock)
          blockchainUpdateTriggers.onProcessMicroBlock(microBlock, detailedDiff, blockchain, blockId)
          ngState = Some(ng.append(time.correctedTime(), microBlock, diff, carry, totalFee))
          log.info(s"$microBlock appended with id $blockId")
          publishLastBlockInfo()

          for {
            (addr, p) <- diff.portfolios
            assetId   <- p.assetIds
          } spendableBalanceChanged.onNext(addr -> assetId)
          blockId
        }
    }

  override def microBlock(id: BlockId): Option[MicroBlock] =
    for {
      ng <- ngState
      mb <- ng.microBlockForId(id)
    } yield mb

  override def microBlockIds: Seq[BlockId] =
    ngState.fold(Seq.empty[BlockId])(_.microBlockIds)

  override def bestLastBlockInfo(maxTimestamp: Long): Option[BlockMinerInfo] =
    ngState
      .map(_.bestLastBlockInfo(maxTimestamp))
      .orElse(
        leveldb.lastBlockHeader.map { sh =>
          BlockMinerInfo(sh.header.baseTarget, sh.header.generationSignature, sh.header.timestamp, sh.id())
        }
      )

  def shutdown(): Unit = {
    internalLastBlockInfo.onComplete()
  }

  private[this] object metrics {
    val blockMicroForkStats       = Kamon.counter("blockchain-updater.block-micro-fork")
    val microMicroForkStats       = Kamon.counter("blockchain-updater.micro-micro-fork")
    val microBlockForkStats       = Kamon.counter("blockchain-updater.micro-block-fork")
    val microBlockForkHeightStats = Kamon.histogram("blockchain-updater.micro-block-fork-height")
    val forgeBlockTimeStats       = Kamon.timer("blockchain-updater.forge-block-time")
  }
}

object BlockchainUpdaterImpl {
  private def diff(p1: Map[Address, Portfolio], p2: Map[Address, Portfolio]) = Monoid.combine(p1, p2.map { case (k, v) => k -> v.negate })

  private def displayFeatures(s: Set[Short]): String =
    s"FEATURE${if (s.size > 1) "S" else ""} ${s.mkString(", ")} ${if (s.size > 1) "have been" else "has been"}"

  def areVersionsOfSameBlock(b1: Block, b2: Block): Boolean =
    b1.header.generator == b2.header.generator &&
      b1.header.baseTarget == b2.header.baseTarget &&
      b1.header.reference == b2.header.reference &&
      b1.header.timestamp == b2.header.timestamp
}
