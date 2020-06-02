package spark.jobserver

import com.typesafe.config.Config
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.codehaus.jackson.map.ObjectMapper
import org.scalactic._
import org.slf4j.{Logger, LoggerFactory}
import spark.jobserver.api.{JobEnvironment, ValidationProblem}

import scala.io.Source

abstract class BaseSparkSessionJob extends SparkSessionJob {

  override type JobData = Config
  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  override def validate(spark: SparkSession, runtime: JobEnvironment, config: Config):
    JobData Or Every[ValidationProblem] = Good(config)
}

/**
  * Load and cache table for sharing within context.
  */
object LoadAndCacheTableJob extends BaseSparkSessionJob {

  type JobOutput = Unit

  private val configPath = "load_table_config.json"
  private val configStr: String = fileToString(configPath)

  override def runJob(spark: SparkSession, runtime: JobEnvironment, config: JobData): JobOutput = {
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
    sc.conf.set("spark.sql.adaptive.join.enabled", "true")
    sc.conf.set("spark.sql.adaptive.skewedJoin.enabled", "true")

    sc.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
    sc.conf.set("spark.sql.autoBroadcastJoinThreshold", "178257920")
    sc.conf.set("spark.sql.broadcastTimeout", "1800")

    sc.conf.set("spark.shuffle.file.buffer", "128k")
    sc.conf.set("spark.shuffle.io.maxRetries", "10")
    sc.conf.set("spark.shuffle.io.retryWait", "60s")
    sc.conf.set("spark.reducer.maxSizeInFlight", "96m")
  }

  private def loadAndCacheTable(sc: SparkSession, tableInfo: TableInfo): Unit = {
    loadAndCacheTable(sc,
      fileToString(tableInfo.getSqlFile), tableInfo.getTableName, tableInfo.getTempViewName)
  }

  private def loadAndCacheTable(
    sc: SparkSession, loadTableSql: String, tableName: String, tempViewName: String = ""): Unit = {
    val df = sc.sql(loadTableSql)
    df.persist(StorageLevel.MEMORY_ONLY_SER)
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
object RunSqlWithNoOutputJob extends BaseSparkSessionJob {

  type JobOutput = Unit

  override def runJob(spark: SparkSession, runtime: JobEnvironment, config: JobData): JobOutput = {
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
object RunSqlWithOutputJob extends BaseSparkSessionJob {

  type JobOutput = Array[Row]

  override def runJob(spark: SparkSession, runtime: JobEnvironment, config: JobData): JobOutput = {
    spark.sql(config.getString("sql")).collect()
  }
}

/**
 * Un-persist cache table.
 */
object UnPersistJob extends BaseSparkSessionJob {

  type JobOutput = Unit

  override def runJob(spark: SparkSession, runtime: JobEnvironment, config: JobData): JobOutput = {
    val tempViewName = config.getString("tempViewName")
    // Drop temp view before un-persist to prevent any un-persisted access.
    spark.catalog.dropTempView(tempViewName)
    spark.catalog.uncacheTable(config.getString("tempViewName"))
  }
}