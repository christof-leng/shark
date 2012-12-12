package shark.execution

import java.util.{HashMap => JavaHashMap, List => JavaList}

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{ExprNodeEvaluator, JoinUtil}
import org.apache.hadoop.hive.ql.exec.{CommonJoinOperator => HiveCommonJoinOperator}
import org.apache.hadoop.hive.ql.plan.{ExprNodeDesc, JoinCondDesc, JoinDesc, TableDesc}
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, PrimitiveObjectInspector}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import shark.SharkConfVars
import spark.RDD
import spark.rdd.UnionRDD
import spark.SparkContext.rddToPairRDDFunctions


abstract class CommonJoinOperator[JOINDESCTYPE <: JoinDesc, T <: HiveCommonJoinOperator[JOINDESCTYPE]]
  extends NaryOperator[T] {

  @BeanProperty var conf: JOINDESCTYPE = _
  // Order in which the results should be output.
  @BeanProperty var order: Array[java.lang.Byte] = _
  // condn determines join property (left, right, outer joins).
  @BeanProperty var joinConditions: Array[JoinCondDesc] = _
  @BeanProperty var numTables: Int = _
  @BeanProperty var nullCheck: Boolean = _

  @transient
  var joinVals: JavaHashMap[java.lang.Byte, JavaList[ExprNodeEvaluator]] = _
  @transient
  var joinFilters: JavaHashMap[java.lang.Byte, JavaList[ExprNodeEvaluator]] = _
  @transient
  var joinValuesObjectInspectors: JavaHashMap[java.lang.Byte, JavaList[ObjectInspector]] = _
  @transient
  var joinFilterObjectInspectors: JavaHashMap[java.lang.Byte, JavaList[ObjectInspector]] = _
  @transient
  var joinValuesStandardObjectInspectors: JavaHashMap[java.lang.Byte, JavaList[ObjectInspector]] = _

  @transient var noOuterJoin: Boolean = _

  override def initializeOnMaster() {
    conf = hiveOp.getConf()

    order = conf.getTagOrder()
    joinConditions = conf.getConds()
    numTables = parentOperators.size
    nullCheck = SharkConfVars.getBoolVar(hconf, SharkConfVars.JOIN_CHECK_NULL)

    assert(joinConditions.size + 1 == numTables)
  }

  override def initializeOnSlave() {

    noOuterJoin = conf.isNoOuterJoin

    joinVals = new JavaHashMap[java.lang.Byte, JavaList[ExprNodeEvaluator]]
    JoinUtil.populateJoinKeyValue(
      joinVals, conf.getExprs(), order, CommonJoinOperator.NOTSKIPBIGTABLE)

    joinFilters = new JavaHashMap[java.lang.Byte, JavaList[ExprNodeEvaluator]]
    JoinUtil.populateJoinKeyValue(
      joinFilters, conf.getFilters(), order, CommonJoinOperator.NOTSKIPBIGTABLE)

    joinValuesObjectInspectors = JoinUtil.getObjectInspectorsFromEvaluators(
      joinVals, objectInspectors.toArray, CommonJoinOperator.NOTSKIPBIGTABLE)
    joinFilterObjectInspectors = JoinUtil.getObjectInspectorsFromEvaluators(
      joinFilters, objectInspectors.toArray, CommonJoinOperator.NOTSKIPBIGTABLE)
    joinValuesStandardObjectInspectors = JoinUtil.getStandardObjectInspectors(
      joinValuesObjectInspectors, CommonJoinOperator.NOTSKIPBIGTABLE)
  }
}

abstract class CartesianProduct[T >: Null : ClassManifest] (numTables: Int) {

  val SINGLE_NULL_LIST = Seq[T](null)
  val EMPTY_LIST = Seq[T]()

  // The output buffer array. The product function returns an iterator that will
  // always return this outputBuffer. Downstream operations need to make sure
  // they are just streaming through the output.
  val outputBuffer = new Array[T](numTables)

  def product(bufs: Array[Seq[T]]): Iterator[Array[T]]
  
  def product2(left: Iterator[Array[T]], right: Seq[T], pos: Int): Iterator[Array[T]] = {
    for (l <- left; r <- right.iterator) yield {
      outputBuffer(pos) = r
      outputBuffer
    }
  }

  def createBase(left: Seq[T], pos: Int): Iterator[Array[T]] = {
    var i = 0
    while (i <= pos) {
      outputBuffer(i) = null
      i += 1
    }
    left.iterator.map { l =>
      outputBuffer(pos) = l
      outputBuffer
    }
  }

  def productBin(left: Seq[T], right: Seq[T]): Iterator[Array[T]] = {
    var l = 0
    var r = 0
    outputBuffer(0) = left(0)

    new Iterator[Array[T]] {
      def hasNext = l < left.size    
      def next = {
        if(r < right.size) {
          outputBuffer(1) = right(r)
          r += 1
        } else {
          r = 0
          outputBuffer(0) = left(l)
          outputBuffer(1) = right(r)
          l += 1
        }
        outputBuffer
      }
    }
  }
}

object CartesianProduct {
  def apply[T >: Null : ClassManifest](joinConditions: Array[JoinCondDesc], numTables: Int) = {
    if(joinConditions.length == 1) {
      joinConditions(0).getType() match {
        case CommonJoinOperator.INNER_JOIN =>
          new BinaryInnerJoin[T](joinConditions(0), numTables)
        case CommonJoinOperator.FULL_OUTER_JOIN =>
          new BinaryFullOuterJoin[T](joinConditions(0), numTables)
        case CommonJoinOperator.LEFT_OUTER_JOIN =>
          new BinaryLeftOuterJoin[T](joinConditions(0), numTables)
        case CommonJoinOperator.RIGHT_OUTER_JOIN =>
          new BinaryRightOuterJoin[T](joinConditions(0), numTables)
        case CommonJoinOperator.LEFT_SEMI_JOIN =>
          new BinaryLeftSemiJoin[T](joinConditions(0), numTables)
      }
    } else {
      new GeneralCartesianProduct[T](joinConditions, numTables)
    }
  }
}

class GeneralCartesianProduct[T >: Null : ClassManifest](val joinConditions: Array[JoinCondDesc], 
  numTables: Int) extends CartesianProduct[T](numTables) {
  
  def product(bufs: Array[Seq[T]]): Iterator[Array[T]] = {

    // This can be done with a foldLeft, but it will be too confusing if we
    // need to zip the bufs with a list of join descriptors...
    var partial: Iterator[Array[T]] = createBase(bufs(joinConditions.head.getLeft), 0)
    var i = 0
    while (i < joinConditions.length) {
      val joinCondition = joinConditions(i)
      i += 1

      partial = joinCondition.getType() match {
        case CommonJoinOperator.INNER_JOIN =>
          if (bufs(joinCondition.getLeft).size == 0 || bufs(joinCondition.getRight).size == 0) {
            createBase(EMPTY_LIST, i)
          } else {
            product2(partial, bufs(joinCondition.getRight), i)
          }

        case CommonJoinOperator.FULL_OUTER_JOIN =>
          if (bufs(joinCondition.getLeft()).size == 0 || !partial.hasNext) {
            // If both right/left are empty, then the right side returns an empty
            // iterator and product2 also returns an empty iterator.
            product2(createBase(SINGLE_NULL_LIST, i - 1), bufs(joinCondition.getRight), i)
          } else if (bufs(joinCondition.getRight).size == 0) {
            product2(partial, SINGLE_NULL_LIST, i)
          } else {
            product2(partial, bufs(joinCondition.getRight), i)
          }

        case CommonJoinOperator.LEFT_OUTER_JOIN =>
          if (bufs(joinCondition.getLeft()).size == 0) {
            createBase(EMPTY_LIST, i)
          } else if (bufs(joinCondition.getRight).size == 0) {
            product2(partial, SINGLE_NULL_LIST, i)
          } else {
            product2(partial, bufs(joinCondition.getRight), i)
          }

        case CommonJoinOperator.RIGHT_OUTER_JOIN =>
          if (bufs(joinCondition.getRight).size == 0) {
            createBase(EMPTY_LIST, i)
          } else if (bufs(joinCondition.getLeft).size == 0 || !partial.hasNext) {
            product2(createBase(SINGLE_NULL_LIST, i - 1), bufs(joinCondition.getRight), i)
          } else {
            product2(partial, bufs(joinCondition.getRight), i)
          }

        case CommonJoinOperator.LEFT_SEMI_JOIN =>
          // For semi join, we only need one element from the table on the right
          // to verify an row exists.
          if (bufs(joinCondition.getLeft).size == 0 || bufs(joinCondition.getRight).size == 0) {
            createBase(EMPTY_LIST, i)
          } else {
            product2(partial, SINGLE_NULL_LIST, i)
          }
      }
    }
    partial
  }
}

class BinaryInnerJoin[T >: Null : ClassManifest](joinCondition: JoinCondDesc, numTables: Int) extends CartesianProduct[T](numTables) {
  val left = joinCondition.getLeft
  val right = joinCondition.getRight

  def product(bufs: Array[Seq[T]]): Iterator[Array[T]] = productBin(bufs(left), bufs(right))
}

class BinaryFullOuterJoin[T >: Null : ClassManifest](joinCondition: JoinCondDesc, numTables: Int) extends CartesianProduct[T](numTables) {
  val left = joinCondition.getLeft
  val right = joinCondition.getRight

  def product(bufs: Array[Seq[T]]): Iterator[Array[T]] = {
    if (bufs(left).size == 0) {
      // If both right/left are empty, then the right side returns an empty
      // iterator and product2 also returns an empty iterator.
      productBin(SINGLE_NULL_LIST, bufs(right))
    } else if (bufs(right).size == 0) {
      productBin(bufs(left), SINGLE_NULL_LIST)
    } else {
      productBin(bufs(left), bufs(right))
    }
  }
}
          
class BinaryLeftOuterJoin[T >: Null : ClassManifest](joinCondition: JoinCondDesc, numTables: Int) extends CartesianProduct[T](numTables) {
  val left = joinCondition.getLeft
  val right = joinCondition.getRight

  def product(bufs: Array[Seq[T]]): Iterator[Array[T]] = {
    if (bufs(left).size == 0) {
      Seq[Array[T]]().iterator
    } else if (bufs(right).size == 0) {
      productBin(bufs(left), SINGLE_NULL_LIST)
    } else {
      productBin(bufs(left), bufs(right))
    }
  }
}
          
class BinaryRightOuterJoin[T >: Null : ClassManifest](joinCondition: JoinCondDesc, numTables: Int) extends CartesianProduct[T](numTables) {
  val left = joinCondition.getLeft
  val right = joinCondition.getRight

  def product(bufs: Array[Seq[T]]): Iterator[Array[T]] = {
    if (bufs(right).size == 0) {
      Seq[Array[T]]().iterator
    } else if (bufs(left).size == 0) {
      productBin(SINGLE_NULL_LIST, bufs(right))
    } else {
      productBin(bufs(left), bufs(right))
    }
  }
}

class BinaryLeftSemiJoin[T >: Null : ClassManifest](joinCondition: JoinCondDesc, numTables: Int) extends CartesianProduct[T](numTables) {
  val left = joinCondition.getLeft
  val right = joinCondition.getRight

  def product(bufs: Array[Seq[T]]): Iterator[Array[T]] = {
    // For semi join, we only need one element from the table on the right
    // to verify an row exists.
    if (bufs(left).size == 0 || bufs(right).size == 0) {
      Seq[Array[T]]().iterator
    } else {
      productBin(bufs(left), SINGLE_NULL_LIST)
    }
  }
}

object CommonJoinOperator {

  val NOTSKIPBIGTABLE = -1

  // Different join types.
  val INNER_JOIN = JoinDesc.INNER_JOIN
  val LEFT_OUTER_JOIN = JoinDesc.LEFT_OUTER_JOIN
  val RIGHT_OUTER_JOIN = JoinDesc.RIGHT_OUTER_JOIN
  val FULL_OUTER_JOIN = JoinDesc.FULL_OUTER_JOIN
  val UNIQUE_JOIN = JoinDesc.UNIQUE_JOIN // We don't support UNIQUE JOIN.
  val LEFT_SEMI_JOIN = JoinDesc.LEFT_SEMI_JOIN

  /**
   * Handles join filters in Hive. It is kind of buggy and not used at the moment.
   */
  def isFiltered(row: Any, filters: JavaList[ExprNodeEvaluator], ois: JavaList[ObjectInspector])
  : Boolean = {
    var ret: java.lang.Boolean = false
    var j = 0
    while (j < filters.size) {
      val condition: java.lang.Object = filters.get(j).evaluate(row)
      ret = ois.get(j).asInstanceOf[PrimitiveObjectInspector].getPrimitiveJavaObject(
        condition).asInstanceOf[java.lang.Boolean]
      if (ret == null || !ret) {
        return true;
      }
      j += 1
    }
    false
  }

  /**
   * Determines the order in which the tables should be joined (i.e. the order
   * in which we produce the Cartesian products).
   */
  def computeTupleOrder(joinConditions: Array[JoinCondDesc]): Array[Int] = {
    val tupleOrder = new Array[Int](joinConditions.size + 1)
    var pos = 0

    def addIfNew(table: Int) {
      if (!tupleOrder.contains(table)) {
        tupleOrder(pos) = table
        pos += 1
      }
    }

    joinConditions.foreach { joinCond =>
      addIfNew(joinCond.getLeft())
      addIfNew(joinCond.getRight())
    }
    tupleOrder
  }
}

