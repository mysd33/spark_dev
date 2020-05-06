package com.example.app

import com.example.fw.app.DatabricksConnectApplicationEntryPoint
import com.example.fw.domain.utils.ResourceBundleManager
import org.apache.spark.SparkContext

object EntryPoint extends DatabricksConnectApplicationEntryPoint {
  def main(args: Array[String]): Unit = {
    //Databricks上でテストデータをnotebookで、dbutilでDatabricks上にマウントしてから実行すること
    //notebooks/Users/admin@mysd33.work/adls-init-mount.scalaのコードと、サイトを参考にすること
    //参考
    //https://docs.microsoft.com/ja-jp/azure/azure-databricks/databricks-extract-load-sql-data-warehouse?toc=/azure/databricks/toc.json&bc=/azure/databricks/breadcrumb/toc.json
    //https://docs.microsoft.com/ja-jp/azure/databricks/data/data-sources/azure/azure-datalake-gen2
    run(args)
  }

  override def addJar(sc: SparkContext): Unit = {
    //DatabricksConnectの場合、必要なクラスの入ったjarを追加する必要があるのでsbt assemblyを実行しておくこと
    sc.addJar("target/scala-2.11/databricks_dev-assembly-0.1.jar")
 }
}
