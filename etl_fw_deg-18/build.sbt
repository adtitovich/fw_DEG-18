name := "ETL_FW_DEG-18"

version := "1.0"

javacOptions ++= Seq("-source", "17", "-target", "17")

scalaVersion := "2.13.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.4.1"
