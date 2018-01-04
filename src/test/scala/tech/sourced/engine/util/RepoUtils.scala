package tech.sourced.engine.util

import java.nio.file.{Path, Paths}

import org.apache.commons.io.FileUtils
import org.eclipse.jgit.api.CreateBranchCommand.SetupUpstreamMode
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.revwalk.RevCommit
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

  def commitFile(repo: Git, name: String, content: String, msg: String): RevCommit = {
    val file = Paths.get(repo.getRepository.getDirectory.getParent, name)
    FileUtils.write(file.toFile, content)
    repo.add().addFilepattern(name).call()
    repo.commit().setMessage(msg).call()
  }

}
