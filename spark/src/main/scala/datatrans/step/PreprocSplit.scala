package datatrans.step

import datatrans.GeoidFinder
import java.util.concurrent.atomic.AtomicInteger
import datatrans.Utils._
import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.types._
import org.joda.time._

import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._
import org.apache.log4j.{Logger, Level}

import net.jcazevedo.moultingyaml._

import datatrans.environmentaldata._
import datatrans.environmentaldata.Utils._
import datatrans.Config._
import datatrans.Implicits._
import datatrans._


case class SplitConfig(
  input_file : String,
  output_file : String,
  splitIndices: Seq[String],
  indices : Seq[String]
) extends StepConfig

object PreprocSplitYamlProtocol extends SharedYamlProtocol {
  implicit val preprocSplitYamlFormat = yamlFormat4(SplitConfig)
}

/**
  *  split preagg into individual files for patients 
  */
object PreprocSplit extends StepConfigConfig {

  type ConfigType = SplitConfig

  val yamlFormat = PreprocSplitYamlProtocol.preprocSplitYamlFormat

  val configType = classOf[SplitConfig].getName()

  val log = Logger.getLogger(getClass.getName)

  log.setLevel(Level.INFO)

  def step(spark: SparkSession, config: SplitConfig) = {
    time {
      import spark.implicits._

      val patient_dimension = config.input_file
      log.info("loading patient_dimension from " + patient_dimension)
      val df = spark.read.format("csv").option("header", value = true).load(patient_dimension)

      val hc = spark.sparkContext.hadoopConfiguration

      val output_file = config.output_file

      val indices = config.indices

      val df_with_year2 = df_with_year.select((splitIndices +: indices).map(df_with_year.col(_)):_*)

      df_with_year2.write.partitionBy(splitIndices : _*).format("csv").save(output_file)
     }
  }

}
