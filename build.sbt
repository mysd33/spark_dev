ThisBuild / scalaVersion := "2.11.0"
ThisBuild / version := "0.1"
ThisBuild / organization := "com.example"

lazy val sparkVersion = "2.4.5"
lazy val scalatestVersion = "3.1.1"
//lazy val unmanagedJarFiles = "c:\\programdata\\anaconda3\\lib\\site-packages\\pyspark\\jars"
lazy val unmanagedJarFiles = "c:\\users\\masas\\.conda\\envs\\dbconnect\\lib\\site-packages\\pyspark\\jars"
//TODO テスト時のAP基盤のプロファイル設定
Test / fork := true
Test / javaOptions += "-Dactive.profile=ut"

lazy val commonSettings = Seq(
  //共通のsettingsを記述
  assemblyOption in assembly := (assemblyOption in assembly).value
    .copy(includeScala = false, includeDependency = false),
)

lazy val root = (project in file("."))
  .aggregate(application, sparkFramework, databricksFramework, dbconnectApplication)
  .dependsOn(application, databricksFramework)
  .settings(
    commonSettings,
    name := "databricks_dev"
  )

lazy val dbconnectApplication = (project in file("dbconnectApplication"))
  .dependsOn(application, databricksFramework)
  .settings(
    commonSettings,
    name := "dbconnectApplication",
    //Dependency Library Setting For Databricks Connect
    //https://docs.microsoft.com/ja-jp/azure/databricks/dev-tools/databricks-connect#sbt
    //https://www.scala-sbt.org/1.x/docs/Library-Management.html
    unmanagedBase := new java.io.File(unmanagedJarFiles),
    //https://www.scala-sbt.org/1.x/docs/Library-Management.html#Exclude+Transitive+Dependencies
    //Databricks接続用のライブラリが優先されるようにSparkライブラリの依存関係を除外
    excludeDependencies ++= Seq(
      ExclusionRule(organization = "org.apache.spark")
    )
  )

lazy val application = (project in file("application"))
  .dependsOn(sparkFramework)
  .dependsOn(sparkTestFramework % "test->test;compile->compile")
  .settings(
    commonSettings,
    name := "application",
    version := "0.1",

  )

lazy val databricksFramework = (project in file("databricksFramework"))
  .dependsOn(sparkFramework)
  .settings(
    commonSettings,
    name := "databricksFramework"
  )

lazy val sparkFramework = (project in file("sparkFramework"))
  .settings(
    commonSettings,
    name := "sparkFramework",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion
    )
  )

lazy val sparkTestFramework = (project in file("sparkTestFramework"))
  .dependsOn(sparkFramework)
  .settings(
    commonSettings,
    name := "sparkTestFramework",
    libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVersion % "test"
  )