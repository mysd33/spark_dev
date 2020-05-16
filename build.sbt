ThisBuild / scalaVersion := "2.11.0"
ThisBuild / version := "0.1"
ThisBuild / organization := "com.example"

lazy val sparkVersion = "2.4.5"
lazy val sparkXmlVersion = "0.9.0"
lazy val scalatestVersion = "3.1.1"
//lazy val unmanagedJarFiles = "c:\\programdata\\anaconda3\\lib\\site-packages\\pyspark\\jars"
//lazy val unmanagedJarFiles = "c:\\users\\masas\\.conda\\envs\\dbconnect\\lib\\site-packages\\pyspark\\jars"
lazy val unmanagedJarFiles = "lib"

lazy val IT_TEST = "it,test"

//sbt test、it:testを実行する場合、プロファイル設定するため
//「sbt -Dactive.profile=ut test」
//「sbt -Dactive.profile=it it:test」
//のように起動すること
//IntelliJの場合には、設定で、sbtのVM起動オプションを設定しておく。
//ちなみに、sbtのfork機能でのjavaOptions設定は有効にならなかった
lazy val commonSettings = Seq(
  //共通のsettingsを記述
  //sbt assemblyで、scalaのjarや、sbtでlibraryDependencies定義した依存ライブラリのクラスはjarに含めない
  assemblyOption in assembly := (assemblyOption in assembly).value
    .copy(includeScala = false, includeDependency = false),
  //sbt assemblyで、テストをスキップ
  test in assembly := {},
  autoAPIMappings := true
)

lazy val root = (project in file("."))
  .aggregate(application, sparkFramework, databricksFramework, dbconnectApplication)
  .dependsOn(application, databricksFramework)
  //enabled sbt-unidoc
  //https://github.com/sbt/sbt-unidoc#how-to-unify-scaladoc
  .enablePlugins(ScalaUnidocPlugin)
  .settings(
    commonSettings,
    name := "databricks_dev",
    //https://github.com/sbt/sbt-unidoc#how-to-include-multiple-configurations
    unidocProjectFilter in (ScalaUnidoc, unidoc) := inAnyProject -- inProjects(application, dbconnectApplication)
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
  //Integration Test対応(sbt it:test)
  .configs(IntegrationTest)
  .dependsOn(sparkFramework)
  .dependsOn(sparkTestFramework % "it->test;test->test;compile->compile")
  .settings(
    Defaults.itSettings,
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
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      //TODO: spark-xmlは複雑なXMLを扱うのに適していないので本番開発では使用しない
      "com.databricks" %% "spark-xml" % sparkXmlVersion
      //TODO: 以下のspark-xmlの依存jarをすべてDatabricksクラスタにインストールしないと動作しないので注意
      //spark-xml_2.11.0-0.9.0.jar, txw2-2.3.2.jar
      //Databricksの場合Mavenリポジトリから直接ライブラリ登録も可能
      //com.databricks:spark-xml_2.11:0.9.0
      //org.glassfish.jaxb:txw2:2.3.2

    )
  )

lazy val sparkTestFramework = (project in file("sparkTestFramework"))
  //Integration Test対応(sbt it:test)
  .configs(IntegrationTest)
  .dependsOn(sparkFramework)
  .settings(
    commonSettings,
    name := "sparkTestFramework",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalatestVersion % IT_TEST,
      "org.mockito" % "mockito-core" % "3.3.3" % IT_TEST,
      "org.scalatestplus" %% "scalatestplus-mockito" % "1.0.0-M2" % IT_TEST
    )
  )

