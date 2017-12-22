package tech.sourced.engine.util

import java.nio.file.Path

import org.eclipse.jgit.api.Git
import org.eclipse.jgit.transport.URIish

object RepoUtils {

  def createBareRepo(path: Path): Git = {
    Git.init().setBare(true).setDirectory(path.toFile).call()
  }

  def createRepo(path: Path): Git = {
    Git.init().setDirectory(path.toFile).call()
  }

  def addRemote(repo: Git, name: String, url: String): Unit = {
    val cmd = repo.remoteAdd()
    cmd.setName(name)
    cmd.setUri(new URIish(url))
    cmd.call()
  }
}
