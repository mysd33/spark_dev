ThisBuild / scalaVersion := "2.11.0"
ThisBuild / organization := "com.example"

//val unmanagedJarFiles = "c:\\programdata\\anaconda3\\lib\\site-packages\\pyspark\\jars"
val unmanagedJarFiles = "c:\\users\\masas\\.conda\\envs\\dbconnect\\lib\\site-packages\\pyspark\\jars"
val sparkVersion = "2.4.5"

//TODO:ジョブの Jar を作成するときにライブラリの依存関係を扱う場合は、Spark と Hadoop を provided 依存関係にする
lazy val root = (project in file("."))
  .aggregate(sparkFramework_db)
  .aggregate(app)
  .dependsOn(sparkFramework_db)
  .dependsOn(app)
  .settings(
    name := "databricks_dev",
    version := "0.1"
  )

lazy val dbconnect_app = (project in file("dbconnect_ap"))
  .settings(
    name := "dbconnect_app",
    version := "0.1",
    unmanagedBase := new java.io.File(unmanagedJarFiles)
  )

lazy val app = (project in file("application"))
  .aggregate(sparkFramework)
  .dependsOn(sparkFramework)
  .settings(
    name := "application",
    version := "0.1"
  )


lazy val sparkFramework = (project in file("sparkFramework"))
  .settings(
    name := "sparkFramework",
    version := "0.1",
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
  )

lazy val sparkFramework_db = (project in file("sparkFramework_databricks"))
  .aggregate(sparkFramework)
  .dependsOn(sparkFramework)
  .settings(
    name :="sparkFramework_databricks",
    version := "0.1"
  )
