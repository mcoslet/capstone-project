ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "Capstone"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.0",
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "org.scalatest" %% "scalatest" % "3.2.12" % "test",
  "org.rogach" %% "scallop" % "4.1.0"
)
compileOrder := CompileOrder.JavaThenScala
