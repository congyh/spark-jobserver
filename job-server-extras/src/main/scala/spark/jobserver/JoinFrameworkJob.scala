package spark.jobserver

import com.typesafe.config.Config
import org.apache.spark.sql.{Row, SparkSession}
import org.codehaus.jackson.map.ObjectMapper
import org.scalactic._
import org.slf4j.LoggerFactory
import spark.jobserver.api.{JobEnvironment, ValidationProblem}

import scala.io.Source

/**
  * Load dim table for sharing.
  */
object LoadDimTableJob extends SparkSessionJob {

  type JobData = Config
  type JobOutput = Unit

  private val configPath = "load_table_config.json"
  private val configStr: String = fileToString(configPath)

  private val logger = LoggerFactory.getLogger(getClass)

  def validate(spark: SparkSession, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem] = Good(config)

  def runJob(spark: SparkSession, runtime: JobEnvironment, config: JobData): JobOutput = {
    prepareSparkSession(spark)

    val objectMapper = new ObjectMapper()
    logger.info(s"configStr: $configStr")
    val loadTableConfig: LoadTableConfig = objectMapper.readValue(configStr, classOf[LoadTableConfig])
    logger.info(s"Parsed config: $loadTableConfig")
    for (tableInfo <- loadTableConfig.getTables) {
      loadAndCacheTable(spark, tableInfo)
    }
  }

  private def prepareSparkSession(sc: SparkSession): Unit = {
    sc.conf.set("spark.sql.adaptive.enabled", "true")
    sc.conf.set("spark.sql.autoBroadcastJoinThreshold", "178257920")
    sc.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
  }

  private def loadAndCacheTable(sc: SparkSession, tableInfo: TableInfo): Unit = {
    loadAndCacheTable(sc,
      fileToString(tableInfo.getSqlFile), tableInfo.getTableName, tableInfo.getTempViewName)
  }

  private def loadAndCacheTable(
    sc: SparkSession, loadTableSql: String, tableName: String, tempViewName: String = ""): Unit = {
    val df = sc.sql(loadTableSql)
    df.cache()
    df.count()
    val viewName = if (tempViewName.equals("")) {
      tableName.substring(tableName.lastIndexOf(".") + 1)
    } else {
      tempViewName
    }
    df.createOrReplaceTempView(viewName)
  }

  private def fileToString(filePath: String): String = {
    Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(filePath)).getLines().mkString(" ")
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
