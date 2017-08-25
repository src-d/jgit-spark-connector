package tech.sourced.api

import java.io.File
import java.net.URL
import java.nio.file.Paths

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.eclipse.jgit.storage.file.FileRepositoryBuilder
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class RDDSpec extends FlatSpec with Matchers {
  "RepositoriesRDD" should "create an rdd with all the repositories in a specified folder" in {
    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()

    //FileSystem.get(spark.sparkContext.hadoopConfiguration).listFiles()

    val localRepositoriesPath = "/tmp/processing-repositories"

    val path = "file:/home/antonio/test-repos/"

    // TODO found a better way to do this
    val wildcards = "/*" :: "/*/*" :: "/*/*/*" :: Nil

    val pathContent = spark.sparkContext.binaryFiles(wildcards.map(w => path + w).mkString(","))

    val result = pathContent
      .map((fileData) => {
        val relativeFilePath = Paths.get(new URL(path).toURI)
          .relativize(Paths.get(new URL(fileData._1).toURI))
        val repositoryName = relativeFilePath.getName(0).toString

        (repositoryName, (relativeFilePath.toString, fileData._2))
      })
      .groupByKey()
      .map((repoFiles) => {
        repoFiles._2.foreach((fileInfo) => {
          val relativeFilePath = fileInfo._1
          val pds = fileInfo._2
          val dest = Paths.get(localRepositoriesPath, relativeFilePath).toFile
          if (!dest.exists()) {
            FileUtils.copyInputStreamToFile(pds.open(), dest)
          }
        })

        val localRepoFolder = Paths.get(localRepositoriesPath, repoFiles._1).toFile
        new File(localRepoFolder, "refs").mkdir()

        // TODO create a singleton per each JVM
        new FileRepositoryBuilder().setGitDir(localRepoFolder).build()

        // TODO do all the operations in the same map, FileRepositoryBuilder can be not serializable
      })

    result.foreach(repo => println(repo.getAllRefs.keySet().asScala.mkString(",")))
  }

}

