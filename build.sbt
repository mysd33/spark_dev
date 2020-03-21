ThisBuild / scalaVersion := "2.11.0"
ThisBuild / organization := "com.example"

//val unmanagedJarFiles = "c:\\programdata\\anaconda3\\lib\\site-packages\\pyspark\\jars"
val unmanagedJarFiles = "c:\\users\\masas\\.conda\\envs\\dbconnect\\lib\\site-packages\\pyspark\\jars"

lazy val root = (project in file(".") )
    .settings(
      name := "databricks_dev",
      version := "0.1",
      unmanagedBase := new java.io.File(unmanagedJarFiles)
    )


