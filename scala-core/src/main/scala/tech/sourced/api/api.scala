package tech.sourced

package object api {
  type SHA1 = String
  /* FIXME: UUID mapped to String to avoid serialization issues */
  type UUID = String
}
