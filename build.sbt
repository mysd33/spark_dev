ThisBuild / scalaVersion := "2.11.0"
ThisBuild / version := "0.1"
ThisBuild / organization := "com.example"

lazy val sparkVersion = "2.4.5"
lazy val scalatestVersion = "3.1.1"
//lazy val unmanagedJarFiles = "c:\\programdata\\anaconda3\\lib\\site-packages\\pyspark\\jars"
lazy val unmanagedJarFiles = "c:\\users\\masas\\.conda\\envs\\dbconnect\\lib\\site-packages\\pyspark\\jars"

//sbt testのプロファイル設定するため
//sbt -Dactive.profile=ut test以下のように起動すること
//IntelliJの場合には、設定で、sbtのVM起動オプションを設定しておく。
//ちなみに、sbtのfork機能でのjavaOptions設定は有効にならなかった

lazy val commonSettings = Seq(
  //共通のsettingsを記述
  //sbt assemblyで、scalaのjarや、sbtでlibraryDependencies定義した依存ライブラリのクラスはjarに含めない
  assemblyOption in assembly := (assemblyOption in assembly).value
    .copy(includeScala = false, includeDependency = false),
  //sbt assemblyで、テストをスキップ
  test in assembly := {}
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
    version := "0.1"
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
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "org.mockito" % "mockito-core" % "3.3.3" % Test,
      "org.scalatestplus" %% "scalatestplus-mockito" % "1.0.0-M2" % Test
    )
  )