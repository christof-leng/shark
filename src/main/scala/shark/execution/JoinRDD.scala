package spark

import java.util.{HashMap => JHashMap, List => JList}
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import shark.execution.OperatorSerializationWrapper
import shark.execution.JoinOperator
import shark.execution.ReduceKey
import shark.execution.CartesianProduct
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.hive.serde2.SerDeUtils

// A version of JoinedRDD with the following changes:
// - Disable map-side aggregation.
// - Enforce return type to Array[ArrayBuffer].
// - 

sealed trait JoinSplitDep extends Serializable
case class NarrowJoinSplitDep(rdd: RDD[_], split: Split) extends JoinSplitDep
case class ShuffleJoinSplitDep(shuffleId: Int) extends JoinSplitDep


class JoinSplit(idx: Int, val deps: Seq[JoinSplitDep]) extends Split with Serializable {
  override val index: Int = idx
  override def hashCode(): Int = idx
}

// Disable map-side combine for the aggregation.
class JoinAggregator
  extends Aggregator[Any, Any, ArrayBuffer[Any]](
    { x => ArrayBuffer(x) },
    { (b, x) => b += x },
    null,
    false)
  with Serializable

class JoinRDD(@transient rdds: Seq[RDD[(_, _)]], part: Partitioner, 
    op: OperatorSerializationWrapper[JoinOperator])
  extends RDD[Array[Object]](rdds.head.context) with Logging {

  val aggr = new JoinAggregator

  @transient
  override val dependencies = {
    val deps = new ArrayBuffer[Dependency[_]]
    for ((rdd, index) <- rdds.zipWithIndex) {
      if (rdd.partitioner == Some(part)) {
        logInfo("Adding one-to-one dependency with " + rdd)
        deps += new OneToOneDependency(rdd)
      } else {
        logInfo("Adding shuffle dependency with " + rdd)
        deps += new ShuffleDependency[Any, Any, ArrayBuffer[Any]](rdd, Some(aggr), part)
      }
    }
    deps.toList
  }

  @transient
  val splits_ : Array[Split] = {
    val firstRdd = rdds.head
    val array = new Array[Split](part.numPartitions)
    for (i <- 0 until array.size) {
      array(i) = new JoinSplit(i, rdds.zipWithIndex.map { case (r, j) =>
        dependencies(j) match {
          case s: ShuffleDependency[_, _, _] =>
            new ShuffleJoinSplitDep(s.shuffleId): JoinSplitDep
          case _ =>
            new NarrowJoinSplitDep(r, r.splits(i)): JoinSplitDep
        }
      }.toList)
    }
    array
  }

  override def splits = splits_

  override val partitioner = Some(part)

  override def preferredLocations(s: Split) = Nil

  override def compute(s: Split): Iterator[Array[Object]] = {
    val split = s.asInstanceOf[JoinSplit]
    val numRdds = split.deps.size
    val map = new JHashMap[ReduceKey, Array[ArrayBuffer[Any]]]
    def getSeq(k: ReduceKey): Array[ArrayBuffer[Any]] = {
      var values = map.get(k)
      if (values == null) {
        values = Array.fill(numRdds)(new ArrayBuffer[Any])
        values(0) += null
        map.put(k, values)
      }
      values
    }
    // hash every table *except* the big one
    for ((dep, depNum) <- split.deps.zipWithIndex.tail) dep match {
      case NarrowJoinSplitDep(rdd, itsSplit) => {
        // Read them from the parent
        for ((k, v) <- rdd.iterator(itsSplit)) { getSeq(k.asInstanceOf[ReduceKey])(depNum) += v }
      }
      case ShuffleJoinSplitDep(shuffleId) => {
        // Read map outputs of shuffle
        def mergePair(k: ReduceKey, v: Any) { getSeq(k)(depNum) += v }
        val fetcher = SparkEnv.get.shuffleFetcher
        fetcher.fetch[ReduceKey, Any](shuffleId, split.index, mergePair)
      }
    }
      
    op.initializeOnSlave()

    val bigTable = split.deps.head match {
      // Read them from the parent
      case NarrowJoinSplitDep(rdd, itsSplit) => rdd.iterator(itsSplit)
      // Read map outputs of shuffle
      case ShuffleJoinSplitDep(shuffleId) => {
        val buf = new ArrayBuffer[(ReduceKey, Any)]
        def mergePair(k: ReduceKey, v: Any) { buf += Tuple2(k, v) }
        val fetcher = SparkEnv.get.shuffleFetcher
        fetcher.fetch[ReduceKey, Any](shuffleId, split.index, mergePair)
        buf.iterator
      }
    }
    
    val tmp = new Array[Object](2)
    val writable = new BytesWritable
    val nullSafes = op.conf.getNullSafes()

    val cp = CartesianProduct[Any](op.joinConditions, op.numTables)

    bigTable.flatMap { case (k: ReduceKey, v: Array[Byte]) =>
      writable.set(k.bytes, 0, k.bytes.length)

      val bufs = getSeq(k)
      // put the current record of the big table back in
      bufs(0)(0) = v
      
      // If nullCheck is false, we can skip deserializing the key.
      if (op.nullCheck &&
          SerDeUtils.hasAnyNullObject(
            op.keyDeserializer.deserialize(writable).asInstanceOf[JList[_]],
            op.keyObjectInspector,
            nullSafes)) {
        bufs.zipWithIndex.flatMap { case (buf, label) =>
          val bufsNull = Array.fill(op.numTables)(ArrayBuffer[Any]())
          bufsNull(label) = buf
          op.generateTuples(cp.product(bufsNull.asInstanceOf[Array[Seq[Any]]]))
        }
      } else {
        op.generateTuples(cp.product(bufs.asInstanceOf[Array[Seq[Any]]]))
      }
    }
  }
}
