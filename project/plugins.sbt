logLevel := Level.Warn

resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Spray Repository" at "http://repo.spray.cc/"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.10.2")

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.5.1")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "0.7.1")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.4.0")

addSbtPlugin("com.github.gseitz" % "sbt-protobuf" % "0.3.3")