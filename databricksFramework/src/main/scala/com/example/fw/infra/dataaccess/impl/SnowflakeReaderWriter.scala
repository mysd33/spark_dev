package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.model.DwDmModel
import com.example.fw.domain.utils.ResourceBundleManager
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import scala.reflect.runtime.universe.TypeTag

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

//TODO:動作確認テスト未実施
/**
 * DwhDmModelに対応した、Snowflakeへのテーブルアクセス機能を提供するクラス
 *
 * Azure Databricks上でしか動作しない。また、Snowflakeのセットアップが必要となる。
 *
 * 本機能を利用する際は、Azure KeyVaultまたはDatabricksのシークレットを定義してSnowflakeのユーザ、パスワードを設定すること。
 * 本クラス内部では、Databricksユーティリティ（dbuutils）を使用してシークレットを取得する。
 * application-xxx.propertiesに、以下のシークレット取得のための情報定義すること。
 * {{{
 *   #Snowflake username scope name to get from secret manager
 *   snowflake.username.scope=<scope>
 * 　　#Snowflake username key to get from secret manager
 * 　　snowflake.username.key=<username key>
 * 　　#Snowflake password scope name to get from secret manager
 *   snowflake.password.scope=<scope>
 * 　　#Snowflake pasoward key to get from secret manager
 *   snowflake.password.key=<password key>
 * }}}
 *
 * また、application-xxx.propertiesに、以下のSnowflake関連の接続情報を定義すること。
 * {{{
 * 　　#Snowflake URL
 *   snowflake.url=<The database to use for the session after connecting>
 * 　　#Snowflake Database
 *   snowflake.db=<The database to use for the session after connecting>
 * 　　#Snowflake Schema
 *   snowflake.schema=<The schema to use for the session after connecting>
 * 　　#Snowflake Warehouse
 *   snowflake.warehouse=<The default virtual warehouse to use for the session after connecting>
 * }}}
 *
 * @see [[https://docs.microsoft.com/ja-jp/azure/databricks/data/data-sources/snowflake]]
 * @see [[https://docs.snowflake.com/ja/user-guide/spark-connector-databricks.html]]
 * @see [[https://docs.microsoft.com/ja-jp/azure/databricks/dev-tools/databricks-utils]]
 */
class SnowflakeReaderWriter {
  private val formatName = "snowflake"
  private val SNOWFLAKE_USERNAME_SCOPE = "snowflake.username.scope"
  private val SNOWFLAKE_USERNAME_KEY = "snowflake.username.key"
  private val SNOWFLAKE_PASSWORD_SCOPE = "snowflake.password.scope"
  private val SNOWFLAKE_PASSWORD_KEY = "snowflake.password.key"
  private val SNOWFLAKE_URL = "snowflake.url"
  private val SNOWFLAKE_DB = "snowflake.db"
  private val SNOWFLAKE_SCHEMA = "snowflake.schema"
  private val SNOWFLAKE_WAREHOUSE = "snowflake.warehouse"

  // Snowflakeのユーザ、パスワード情報
  private lazy val user = dbutils.secrets.get(ResourceBundleManager.get(SNOWFLAKE_USERNAME_SCOPE), ResourceBundleManager.get(SNOWFLAKE_USERNAME_KEY))
  private lazy val password = dbutils.secrets.get(ResourceBundleManager.get(SNOWFLAKE_PASSWORD_SCOPE), ResourceBundleManager.get(SNOWFLAKE_PASSWORD_KEY))

  // Snowflakeの接続情報
  private lazy val options =
    Map("sfUrl" -> ResourceBundleManager.get(SNOWFLAKE_URL),
      "sfUser" -> user,
      "sfPassword" -> password,
      "sfDatabase" -> ResourceBundleManager.get(SNOWFLAKE_DB),
      "sfSchema" -> ResourceBundleManager.get(SNOWFLAKE_SCHEMA),
      "sfWarehouse" -> ResourceBundleManager.get(SNOWFLAKE_WAREHOUSE))

  /**
   * Snowflakeのテーブルから読み込みDataFrameを返却する
   *
   * @param inputFile    入力ファイルのDwDmModel。dbTableプロパティに指定したテーブルを取得する。queryプロパティには対応指定しない。
   * @param sparkSession SparkSession
   * @return DataFrame
   */
  def readToDf(inputFile: DwDmModel[Row], sparkSession: SparkSession): DataFrame = {
    assert(inputFile.dbTable.isDefined && inputFile.query.isEmpty, "queryの指定は未対応です")
    sparkSession.read
      .format(formatName)
      .options(options)
      .option("dbtable", inputFile.dbTable.get)
      .load()
  }

  /**
   * Snowflakeのテーブルから読み込みDatasetを返却する
   *
   * @param inputFile    入力ファイルのDwDmModel。dbTableプロパティに指定したテーブルを取得する。queryプロパティには対応指定しない。
   * @param sparkSession SparkSession
   * @tparam T ParquetModelおよびDatasetの型パラメータ
   * @return Dataset
   */
  def readToDs[T <: Product : TypeTag](inputFile: DwDmModel[T], sparkSession: SparkSession): Dataset[T] = {
    import sparkSession.implicits._
    assert(inputFile.dbTable.isDefined && inputFile.query.isEmpty, "queryの指定は未対応です")
    sparkSession.read
      .format(formatName)
      .options(options)
      .option("dbtable", inputFile.dbTable.get)
      .load().as[T]
  }

  /**
   * 引数で受け取ったDataset/DataFrameをSnowflakeのテーブルに書き込む
   *
   * @param ds         出力対象のDataset/DataFrame
   * @param outputFile 出力先ファイルのDwDmModel。dbTableプロパティに指定したテーブルに書き込む。
   * @tparam T DwDmModelの型パラメータ
   */
  def writeFromDsDf[T](ds: Dataset[T], outputFile: DwDmModel[T]): Unit = {
    assert(outputFile.dbTable.isDefined)
    ds.write
      .format(formatName)
      .options(options)
      .option("dbtable", outputFile.dbTable.get)
      .save()
  }
}