ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"


lazy val root = (project in file("."))
  .settings(
    name := "manny_jenkins"
    
  )

mainClass in Compile := Some("LoadFirst")
mainClass in Compile := Some("IncrementalLoad")

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"
