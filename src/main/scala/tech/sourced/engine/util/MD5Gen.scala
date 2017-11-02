package tech.sourced.engine.util

import java.security.MessageDigest
import javax.xml.bind.annotation.adapters.HexBinaryAdapter

/**
  * Convenience wrapper around java [[java.security.MessageDigest]] for easier md5 hashing.
  */
object MD5Gen {
  private val ba = new HexBinaryAdapter()

  /**
    * Hashes the given string using md5.
    *
    * @param s string to hash
    * @return hashed string
    */
  def str(s: String): String = synchronized {
    ba.marshal(MessageDigest.getInstance("MD5").digest(s.getBytes()))
  }
}

