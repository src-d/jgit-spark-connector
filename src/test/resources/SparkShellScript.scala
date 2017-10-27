import tech.sourced.api._


//End-to-end test of packages spark-api.jar intended to be used with ./spark-shell -i SparkShellScript.scala
//it's not part of src/test/scala to avoid compilation during build
spark.version

try {
  val engine = engine(spark, "./src/test/resources/siva-files/")
  val files = engine.getRepositories.getReferences.filter('name === "refs/heads/HEAD").getFiles
  files.show(2)

  val langs = files.classifyLanguages
  langs.show(2)

  val ex = langs.extractUASTs
  ex.show(2)
} catch {
  case e: _ =>
    e.printStackTrace()
    System.exit(2)
}

System.exit(0)
