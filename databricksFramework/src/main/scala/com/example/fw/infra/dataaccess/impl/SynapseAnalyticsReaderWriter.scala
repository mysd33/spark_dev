package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.model.DwDmModel
import com.example.fw.domain.utils.ResourceBundleManager
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.reflect.runtime.universe.TypeTag

//TODO:動作確認テスト未実施
/**
 * DwhDmModelに対応した、Azure Synapse Analyticsへのテーブルアクセス機能を提供するクラス
 *
 * Azure Databricks上でしか動作しない。また、Azure Synapse Analyticsや仲介するBlobストレージのセットアップが必要となる。
 *
 * 本機能を利用する際は、application-xxx.propertiesに、以下のSynapse Analyticsの接続情報を定義すること。
 * {{{
 *   #Synapse Analytics URL
 *   sqldw.url=jdbc:sqlserver://<the-rest-of-the-connection-string>
 *   #Temp directory path on BLob Storage
 *   sqldw.blob.tempdir=wasbs://<your-container-name>@<your-storage-account-name>.blob.core.windows.net/<your-directory-name>
 * }}}
 * また、機能を利用する際は、Azure KeyVaultまたはDatabricksのシークレットを定義して仲介するBlobストレージのユーザ、パスワードを設定すること。
 * 本クラス内部では、Databricksユーティリティ（dbuutils）を使用してシークレットを取得する。
 * application-xxx.propertiesに、以下のシークレット取得のための情報定義すること。
 * {{{
 *   #Storage account name
 *   sqldw.blob.accountkey.name=fs.azure.account.key.<your-storage-account-name>.blob.core.windows.net
 *   #Account-key scope to get from secret manager
 *   sqldw.blob.accountkey.scope=<scope>
 *   #Acount-key key to get from secret manager
 *   sqldw.blob.accountkey.key=<key>
 * }}}
 * @see [[https://docs.microsoft.com/ja-jp/azure/databricks/data/data-sources/azure/synapse-analytics]]
 * @see [[https://docs.microsoft.com/ja-jp/azure/azure-databricks/databricks-extract-load-sql-data-warehouse]]
 */
class SynapseAnalyticsReaderWriter {
  private val formatName = "com.databricks.spark.sqldw"
  private val SQL_DW_URL_KEY = "sqldw.url"
  private val SQL_DW_BLOB_TEMPDIR_KEY = "sqldw.blob.tempdir"

  /**
   * Azure Synapse Analyticsのテーブルから読み込みDataFrameを返却する
   *
   * @param inputFile    入力ファイルのDwDmModel。dbTableプロパティに指定したテーブルまたは、queryプロパティに指定したクエリをもとにデータ取得する。
   * @param sparkSession SparkSession
   * @return DataFrame
   */
  def readToDf(inputFile: DwDmModel[Row], sparkSession: SparkSession): DataFrame = {
    val url = ResourceBundleManager.get(SQL_DW_URL_KEY)
    val tempDir = ResourceBundleManager.get(SQL_DW_BLOB_TEMPDIR_KEY)
    val reader = sparkSession.read
      .format(formatName)
      .option("url", url)
      .option("tempDir", tempDir)
      .option("forwardSparkAzureStorageCredentials", "true")
    val reader2 = inputFile.query match {
      case Some(query) => reader.option("query", query)
      case _ => {
        inputFile.dbTable match {
          case Some(dbTable) => reader.option("dbTable", dbTable)
          case _ => reader
        }
      }
    }
    reader2.load()
  }

  /**
   * Azure Synapse Analyticsのテーブルから読み込みDatasetを返却する
   *
   * @param inputFile    入力ファイルのDwDmModel。dbTableプロパティに指定したテーブルまたは、queryプロパティに指定したクエリをもとにデータ取得する。
   * @param sparkSession SparkSession
   * @tparam T ParquetModelおよびDatasetの型パラメータ
   * @return Dataset
   */
  def readToDs[T <: Product : TypeTag](inputFile: DwDmModel[T], sparkSession: SparkSession): Dataset[T] = {
    import sparkSession.implicits._
    val url = ResourceBundleManager.get(SQL_DW_URL_KEY)
    val tempDir = ResourceBundleManager.get(SQL_DW_BLOB_TEMPDIR_KEY)
    val reader = sparkSession.read
      .format(formatName)
      .option("url", url)
      .option("tempDir", tempDir)
      .option("forwardSparkAzureStorageCredentials", "true")
    val reader2 = inputFile.query match {
      case Some(query) => reader.option("query", query)
      case _ => {
        inputFile.dbTable match {
          case Some(dbTable) => reader.option("dbTable", dbTable)
          case _ => reader
        }
      }
    }
    reader2.load().as[T]
  }

  /**
   * 引数で受け取ったDataset/DataFrameをAzure Synapse Analyticsのテーブルに書き込む
   *
   * @param ds         出力対象のDataset/DataFrame
   * @param outputFile 出力先ファイルのDwDmModel。dbTableプロパティに指定したテーブルに書き込む。
   * @tparam T DwDmModelの型パラメータ
   */
  def writeFromDsDf[T](ds: Dataset[T], outputFile: DwDmModel[T]): Unit = {
    assert(outputFile.dbTable.isDefined)
    val url = ResourceBundleManager.get(SQL_DW_URL_KEY)
    val tempDir = ResourceBundleManager.get(SQL_DW_BLOB_TEMPDIR_KEY)
    ds.write
      .format(formatName)
      .option("url", url)
      .option("tempDir", tempDir)
      .option("forwardSparkAzureStorageCredentials", "true")
      .option("dbTable", outputFile.dbTable.get)
      .save()
  }
}
