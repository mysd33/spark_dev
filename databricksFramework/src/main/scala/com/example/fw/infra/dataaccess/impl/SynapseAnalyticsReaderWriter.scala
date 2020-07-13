package com.example.fw.infra.dataaccess.impl

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.example.fw.domain.model.DwDmModel
import com.example.fw.domain.utils.ResourceBundleManager
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag

/**
 * DwhDmModelに対応した、Azure Synapse Analyticsへのテーブルアクセス機能を提供するクラス
 *
 * Azure Databricks上でしか動作しない。また、Azure Synapse Analyticsや仲介するBlobストレージのセットアップが必要となる。
 *
 * 本機能を利用する際は、application-xxx.propertiesに、以下のSynapse Analyticsの接続情報を定義すること。
 * なお、SQLDWのURL（JDBC接続文字列）にはパスワードが含まれる。また、Blobストレージのアカウントキーなど秘密情報がソースコードに保存されないよう
 * Databricksのシークレットに設定すること。
 * 本クラス内部では、Databricksユーティリティ（dbUtils）を使用してシークレットを取得する。
 * application-xxx.propertiesに、以下のシークレット取得のための情報定義すること。
 * {{{
 *
 *   #Synapse Analytics URL scope to get from Databricks Secret
 *   sqldw.url.scope=<scope>
 *   #Synapse Analytics URL secret key to get from Databricks Secret
 *   sqldw.url.key=<key>
 *   #Temp directory path on BLob Storage
 *   sqldw.blob.tempdir=wasbs://<your-container-name>@<your-storage-account-name>.blob.core.windows.net/<your-directory-name>
 *
 *   #Storage account name
 *   sqldw.blob.accountkey.name=fs.azure.account.key.<your-storage-account-name>.blob.core.windows.net
 *   #Account-key scope to get from secret manager
 *   sqldw.blob.accountkey.scope=<scope>
 *   #Acount-key scecret key to get from Databricks Secret
 *   sqldw.blob.accountkey.key=<key>
 * }}}
 *
 * @see [[https://docs.microsoft.com/ja-jp/azure/databricks/data/data-sources/azure/synapse-analytics]]
 * @see [[https://docs.microsoft.com/ja-jp/azure/azure-databricks/databricks-extract-load-sql-data-warehouse]]
 */
class SynapseAnalyticsReaderWriter {
  private val formatName = "com.databricks.spark.sqldw"
  private val SQL_DW_URL_SCOPE = "sqldw.url.scope"
  private val SQL_DW_URL_KEY = "sqldw.url.key"
  private val SQL_DW_BLOB_TEMPDIR_URL = "sqldw.blob.tempdir.url"

  /**
   * Azure Synapse Analyticsのテーブルから読み込みDataFrameを返却する
   *
   * @param inputFile    入力ファイルのDwDmModel。dbTableプロパティに指定したテーブルまたは、queryプロパティに指定したクエリをもとにデータ取得する。
   * @param sparkSession SparkSession
   * @return DataFrame
   */
  def readToDf(inputFile: DwDmModel[Row], sparkSession: SparkSession): DataFrame = {
    val reader = sparkSession.read
      .format(formatName)
      .option("url", getSynapseAnalyticsUrl())
      .option("tempDir", getBlobTempDirUrl())
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
    val reader = sparkSession.read
      .format(formatName)
      .option("url", getSynapseAnalyticsUrl())
      .option("tempDir", getBlobTempDirUrl())
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
  def writeFromDsDf[T](ds: Dataset[T], outputFile: DwDmModel[T], saveMode: SaveMode): Unit = {
    assert(outputFile.dbTable.isDefined)
    val url = ResourceBundleManager.get(SQL_DW_URL_KEY)
    val tempDir = ResourceBundleManager.get(SQL_DW_BLOB_TEMPDIR_URL)
    ds.write.mode(saveMode)
      .format(formatName)
      .option("url", getSynapseAnalyticsUrl())
      .option("tempDir", getBlobTempDirUrl())
      .option("forwardSparkAzureStorageCredentials", "true")
      .option("dbTable", outputFile.dbTable.get)
      .save()
  }

  /**
   * Synapse AnalyticsのDWHのURLを取得する
   * @return Synapse AnalyticsのDWHのURL
   */
  private def getSynapseAnalyticsUrl() : String = {
    val urlKey = ResourceBundleManager.get(SQL_DW_URL_KEY)
    val urlScope = ResourceBundleManager.get(SQL_DW_URL_SCOPE)
    //DBUtilsはローカルでは動作しないので注意
    dbutils.secrets.get(urlScope, urlKey)
  }

  /**
   * 一時ストレージとなるBLobストレージのディレクトリのURLを取得
   * @return 一時ストレージとなるBLobストレージのディレクトリ
   */
  private def getBlobTempDirUrl() : String = {
    ResourceBundleManager.get(SQL_DW_BLOB_TEMPDIR_URL)
  }
}
