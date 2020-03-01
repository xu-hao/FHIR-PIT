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


case class EnvDataAggregateConfig(
  input_dir : String,
  output_file : String,
  statistics : Seq[String],
  indices : Seq[String]
) extends StepConfig

object PreprocEnvDataAggregateFIPSYamlProtocol extends SharedYamlProtocol {
  implicit val preprocEnvDataAggregateFIPSYamlFormat = yamlFormat4(EnvDataAggregateFIPSConfig)
}

object PreprocEnvDataAggregateFIPS extends StepConfigConfig {

  type ConfigType = EnvDataAggregateFIPSConfig

  val yamlFormat = PreprocEnvDataAggregateFIPSYamlProtocol.preprocEnvDataAggregateFIPSYamlFormat

  val configType = classOf[EnvDataAggregateFIPSConfig].getName()

  val log = Logger.getLogger(getClass.getName)

  log.setLevel(Level.INFO)

  def step(spark: SparkSession, config: EnvDataAggregateFIPSConfig) = {
    time {
      import spark.implicits._

      val patient_dimension = config.input_file
      log.info("loading patient_dimension from " + patient_dimension)
      val df3year_pat = spark.read.format("csv").option("header", value = true).load(patient_dimension)

      val hc = spark.sparkContext.hadoopConfiguration

      val input_dir_path = new Path(config.input_dir)
      val input_dir_file_systen = input_dir_path.getFileSystem(hc)
      val itr = input_dir_file_system.listFiles(input_dir_path, false)
      val output_file = config.output_file

      val indices = config.indices

      val statistics = config.statistics

      while(itr.hasNext) {
        val input_file_path = itr.next().getPath()

        val p = input_file_path.getName()

        log.info(f"aggregating $indices")
        val df3year_pat_aggbyyear = aggregateByYear(spark, df3year_pat, indices, statistics)
        //        df3year_pat_aggbyyear.cache()
        // log.info(f"columns4 = ${df3year_pat_aggbyyear.columns.toSeq}, nrows1 = ${df3year_pat_aggbyyear.count()}")

        val names3 = for (i <- statistics; j <- indices) yield f"${j}_$i"
        val df4 = df3year_pat_aggbyyear.select("patient_num", ("start_date" +: indices) ++ names3 ++ indices.map((s: String) => f"${s}_prev_date"): _*)
        // log.info(f"columns5 = ${df4.columns.toSeq}, nrows1 = ${df4.count()}")

        writeDataframe(hc, output_file.replace("%i", p), df4)
      }
    }
  }

}












