package com.example.model

import enumeratum._
import skunk.Codec
import skunk.codec.all.`enum`
import skunk.data.Type

sealed abstract class Status(override val entryName: String) extends EnumEntry {
  override def toString: String = entryName
}

object Status extends Enum[Status] {

  case object Pending extends Status("pending")
  case object Executed extends Status("executed")

  val values: IndexedSeq[Status] = findValues

  val statusCodec: Codec[Status] =
    enum((status: Status) => status.entryName, Status.withNameOption, Type.text)
}
