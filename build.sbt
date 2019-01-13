name := "globus"

version := "1.2"

scalaVersion := "2.11.12"

// resourceDirectory in Compile := baseDirectory.value / "resources"

// allows us to include spark packages
resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
resolvers += "Typesafe Simple Repository" at "http://repo.typesafe.com/typesafe/simple/maven-releases/"
resolvers += "MavenRepository" at "https://mvnrepository.com/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0" % "provided"
