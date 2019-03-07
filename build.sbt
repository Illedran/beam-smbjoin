import sbt._
import sbt.Keys._

val scioVersion = "0.7.2"
val beamVersion = "2.10.0"
val scalaMacrosVersion = "2.1.1"

lazy val commonSettings = Defaults.coreDefaultSettings ++ Seq(
    organization := "smbjoin",
    // Semantic versioning http://semver.org/
    version := "0.1.0-SNAPSHOT",
    scalaVersion := "2.12.8",
    scalacOptions ++= Seq(
      "-target:jvm-1.8",
      "-deprecation",
      "-feature",
      "-unchecked"
    ),
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
  )
lazy val paradiseDependency =
    "org.scalamacros" % "paradise" % scalaMacrosVersion cross CrossVersion.full
lazy val macroSettings = Seq(
    libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    addCompilerPlugin(paradiseDependency)
  )
lazy val root: Project = project
    .in(file("."))
    .settings(commonSettings)
    .settings(macroSettings)
    .settings(
      name := "smb-scio-test",
      description := "smb-scio-test",
      publish / skip := true,
      libraryDependencies ++= Seq(
        "com.spotify" %% "scio-core" % scioVersion,
        "com.spotify" %% "scio-test" % scioVersion % Test,
        "com.spotify" %% "scio-avro" % scioVersion,
        "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
        //      "org.apache.beam" % "beam-sdks-java-extensions-sorter" % beamVersion,
        //      "org.apache.beam" % "beam-sdks-java-extensions-sketching" % beamVersion,
        // optional dataflow runner
        // "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
        "org.slf4j" % "slf4j-simple" % "1.7.26"
      )
    )
    .enablePlugins(PackPlugin)
lazy val repl: Project = project
    .in(file(".repl"))
    .settings(commonSettings)
    .settings(macroSettings)
    .settings(
      name := "repl",
      description := "Scio REPL for smb-scio-test",
      libraryDependencies ++= Seq("com.spotify" %% "scio-repl" % scioVersion),
      Compile / mainClass := Some("com.spotify.scio.repl.ScioShell"),
      publish / skip := true
    )
    .dependsOn(root)

