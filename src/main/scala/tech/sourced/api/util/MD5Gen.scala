package tech.sourced.api.util

import java.security.MessageDigest
import javax.xml.bind.annotation.adapters.HexBinaryAdapter

object MD5Gen {
  private val md = MessageDigest.getInstance("MD5")

  private val ba = new HexBinaryAdapter()

  def str(s: String): String = {
    md.reset()
    ba.marshal(md.digest(s.getBytes()))
  }
}

