ThisBuild / scalaVersion := "2.11.0"
ThisBuild / organization := "com.example"

//val unmanagedJarFiles = "c:\\programdata\\anaconda3\\lib\\site-packages\\pyspark\\jars"
val unmanagedJarFiles = "c:\\users\\masas\\.conda\\envs\\dbconnect\\lib\\site-packages\\pyspark\\jars"
val sparkVersion = "2.4.5"

lazy val root = (project in file(".") )
    .settings(
      name := "databricks_dev",
      version := "0.1",
      unmanagedBase := new java.io.File(unmanagedJarFiles)
    )

lazy val sparkFramework = (project in file("sparkFramework"))
  .settings(
    name := "sparkFramework",
    version := "0.1",
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
  )
