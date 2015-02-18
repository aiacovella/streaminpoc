
name := """streaming-poc"""

version := "1.0"

scalaVersion := "2.11.4"

libraryDependencies ++= Seq(
  "com.typesafe.akka"           %% "akka-actor"               % "2.3.4"   % Compile,
  "com.typesafe.akka"           %% "akka-testkit"             % "2.3.4"   % Compile,
  "com.typesafe.akka"           %% "akka-stream-experimental" % "1.0-M1"  % Compile,
  "com.typesafe.scala-logging"  %% "scala-logging"            % "3.1.0"   % Compile,
  "org.mongodb"                 %% "casbah"                   % "2.7.4"   % Compile,
  "com.squants"                 %% "squants"                  % "0.4.2"   % Compile,
  "joda-time"                   % "joda-time"                 % "2.1"     % Compile,
  "org.scalatest"               %% "scalatest"                % "2.1.6"   % Test,
  "junit"                       % "junit"                     % "4.11"    % Test,
  "com.novocode"                % "junit-interface"           % "0.10"    % Test
)

testOptions += Tests.Argument(TestFrameworks.JUnit, "-v")
