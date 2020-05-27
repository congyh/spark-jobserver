package spark.jobserver

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalactic._
import spark.jobserver.api.{JobEnvironment, ValidationProblem}

/**
  * Load dim table for sharing.
  */
object LoadDimTableJob extends SparkSessionJob {

  type JobData = Config
  type JobOutput = Unit

  private val getSkuTable =
    """
      |SELECT
      |            max(division_id_new) as division_id_new,
      |            max(division_name_new) as division_name_new,
      |            max(pur_first_dept_cd) as ad_first_dept_id,
      |            max(pur_first_dept_name) as ad_first_dept_name,
      |            max(pur_second_dept_cd) as ad_second_dept_id,
      |            max(pur_second_dept_name) as ad_second_dept_name,
      |            max(pur_third_dept_cd) as ad_third_dept_id,
      |            max(pur_third_dept_name) as ad_third_dept_name,
      |            max(item_first_cate_cd) as item_first_cate_cd,
      |            max(item_first_cate_name) as item_first_cate_name,
      |            max(brand_code) as ad_sku_brand_code,
      |            max(barndname_full) as ad_sku_brandname_full,
      |            max(type) as type_raw,
      |            item_sku_id AS sku_id
      |        FROM
      |            dim.dim_product_daily_item_sku
      |        WHERE
      |            dt = '2020-05-20' and dp = 'active'
      |        group BY
      |            item_sku_id
      |        CLUSTER BY
      |            sku_id
      |""".stripMargin

  def validate(spark: SparkSession, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem] = Good(config)

  def runJob(spark: SparkSession, runtime: JobEnvironment, config: JobData): JobOutput = {
    val skuDf = spark.sql(s"$getSkuTable")
    skuDf.createOrReplaceTempView("dim_product_daily_item_sku")
    spark.catalog.cacheTable("dim_product_daily_item_sku")
  }
}

/**
  * This job simply runs the Hive SQL in the config and output nothing.
  *
  * 1. Usually the sql is an insertion clause.
  * 2. If you want to use the cached table, you should refine the table name to match cached table name,
  *    usually the name is in the format of database name stripped (db_name.table_name -> table_name).
  */
object RunSqlWithNoOutputJob extends SparkSessionJob {
  type JobData = Config
  type JobOutput = Unit

  def validate(spark: SparkSession, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem] = Good(config)

  def runJob(spark: SparkSession, runtime: JobEnvironment, config: JobData): JobOutput = {
    spark.sql(config.getString("sql"))
  }
}

/**
 * This job simply runs the Hive SQL in the config and output result.
 *
 * 1. Sql with Large output is not recommended. 'cause It will affect the stability of spark-jobserver;
 * 2. If you want to use the cached table, you should refine the table name to match cached table name,
 *    usually the name is in the format of database name stripped (db_name.table_name -> table_name).
 */
object RunSqlWithOutputJob extends SparkSessionJob {
  type JobData = Config
  type JobOutput = Array[Row]

  def validate(spark: SparkSession, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem] = Good(config)

  def runJob(spark: SparkSession, runtime: JobEnvironment, config: JobData): JobOutput = {
    spark.sql(config.getString("sql")).collect()
  }
}
