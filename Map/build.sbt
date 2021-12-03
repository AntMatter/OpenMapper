name := "Map"

version := "0.1"

scalaVersion := "2.12.3"

resolvers += "spark-packages" at "https://repos.spark-packages.org/"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-graphx
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.0.1"

// https://mvnrepository.com/artifact/graphframes/graphframes
//libraryDependencies += "graphframes" % "graphframes" % "0.8.2-spark3.0-s_2.12"
libraryDependencies += "graphframes" % "graphframes" % "0.8.0-spark3.0-s_2.12"



artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + module.revision + "." + artifact.extension
}