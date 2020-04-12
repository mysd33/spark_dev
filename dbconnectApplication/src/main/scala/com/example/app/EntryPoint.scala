package com.example.app

import com.example.fw.app.DatabricksConnectApplicationEntryPoint
import com.example.fw.domain.utils.ResourceBundleManager
import org.apache.spark.SparkContext

object EntryPoint extends DatabricksConnectApplicationEntryPoint {

  def main(args: Array[String]): Unit = {
    //TODO:テストデータをnotebookで、dbutilでDatabricks上にマウントしてから実行する
    //参考
    //https://docs.microsoft.com/ja-jp/azure/azure-databricks/databricks-extract-load-sql-data-warehouse?toc=/azure/databricks/toc.json&bc=/azure/databricks/breadcrumb/toc.json
    //https://docs.microsoft.com/ja-jp/azure/databricks/data/data-sources/azure/azure-datalake-gen2
    run(args)
  }

  override protected def addJar(sc: SparkContext): Unit = {
    // 暫定的に依存関係のjarの追加操作を直接記載
    sc.addJar("sparkFramework/target/scala-2.11/sparkframework_2.11-0.1.jar")
    sc.addJar("databricksFramework/target/scala-2.11/databricksFramework_2.11-0.1.jar")
    sc.addJar("application/target/scala-2.11/application_2.11-0.1.jar")
    sc.addJar("dbconnectApplication/target/scala-2.11/dbconnectApplication_2.11-0.1.jar")

    //TODO:暫定コード
    val profile = ResourceBundleManager.getActiveProfile()
    println(s"Active Profile : $profile")
    val basePath = ResourceBundleManager.get("basepath")
    println(s"BasePath : $basePath")
  }
}
