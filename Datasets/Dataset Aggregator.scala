// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.TypedColumn

// COMMAND ----------

val simpleSum = new Aggregator[Int, Int, Int] with Serializable {
  def zero: Int = 0                     // The initial value.
  def reduce(b: Int, a: Int) = b + a    // Add an element to the running total
  def merge(b1: Int, b2: Int) = b1 + b2 // Merge intermediate values.
  def finish(b: Int) = b                // Return the final result.
  def bufferEncoder: Encoder[Int] = Encoders.scalaInt
  def outputEncoder: Encoder[Int] = Encoders.scalaInt}.toColumn

val ds = Seq(1, 2, 3, 4).toDS()
ds.select(simpleSum).collect

// COMMAND ----------

class SumOf[I, N : Numeric : Encoder](f: I => N) extends Aggregator[I, N, N] with Serializable {

  private val numeric = implicitly[Numeric[N]]
  override def zero: N = numeric.zero
  override def reduce(b: N, a: I): N = numeric.plus(b, f(a))
  override def merge(b1: N, b2: N): N = numeric.plus(b1, b2)
  override def finish(reduction: N): N = reduction
  override def bufferEncoder: Encoder[N] = implicitly[Encoder[N]]
  override def outputEncoder: Encoder[N] = implicitly[Encoder[N]]
};

// COMMAND ----------

def sum = new SumOf[(String, Int), Double](_._2.toDouble).toColumn;
val ds = Seq("a" -> 1, "a" -> 3, "b" -> 3).toDS()
ds.groupByKey(_._1).agg(sum).collect()

// COMMAND ----------

def sum = new SumOf[(Int, Long), (Int)](_._2.toInt).toColumn;
val ds = Seq((1, 2L), (2, 3L), (3, 4L), (1, 5L)).toDS()
ds.groupByKey(_._1).agg(sum).collect()

// COMMAND ----------

def sum[I, N : Numeric : Encoder](f: I => N): TypedColumn[I, N] = new SumOf(f).toColumn
val ds = Seq((1, 2L), (2, 3L), (3, 4L), (1, 5L)).toDS()
ds.select(sum[(Int, Long), Long](_._1), sum[(Int, Long), Long](_._2)).collect()

// COMMAND ----------


