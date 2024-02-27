package com.example.model

import enumeratum._
import skunk.Codec
import skunk.codec.all.`enum`
import skunk.data.Type

sealed abstract class Side(override val entryName: String) extends EnumEntry {
  override def toString: String = entryName
}

object Side extends Enum[Side] {

  case object Buy extends Side("buy")
  case object Sell extends Side("sell")

  val values: IndexedSeq[Side] = findValues

  val sideCodec: Codec[Side] =
    enum((side: Side) => side.entryName, Side.withNameOption, Type.text)
}
