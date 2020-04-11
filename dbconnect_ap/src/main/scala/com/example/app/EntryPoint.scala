package com.example.app

import com.example.fw.app.DatabricksConnectApplicationEntryPoint
import com.example.fw.domain.utils.ResourceBundleManager
import org.apache.spark.SparkContext

object EntryPoint extends DatabricksConnectApplicationEntryPoint {

  def main(args: Array[String]): Unit = {
    //TODO:テストデータをnotebookで、dbutilでDatabricks上にマウントしてから実行する
    //参考
    //https://docs.microsoft.com/ja-jp/azure/azure-databricks/databricks-extract-load-sql-data-warehouse?toc=/azure/databricks/toc.json&bc=/azure/databricks/breadcrumb/toc.json
    run(args)
  }

  override protected def addJar(sc: SparkContext): Unit = {
    // 暫定的に依存関係のjarの追加操作を直接記載
    sc.addJar("sparkFramework/target/scala-2.11/sparkframework_2.11-0.1.jar")
    sc.addJar("sparkFramework_databricks/target/scala-2.11/sparkframework_databricks_2.11-0.1.jar")
    sc.addJar("dbconnect_ap/target/scala-2.11/dbconnect_app_2.11-0.1.jar")

    //TODO:暫定コード
    val profile = ResourceBundleManager.getActiveProfile()
    println(s"Active Profile : $profile")
    val basePath = ResourceBundleManager.get("basepath")
    println(s"BasePath : $basePath")
  }
}
