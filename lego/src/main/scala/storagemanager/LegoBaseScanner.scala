package ch.epfl.data
package legobase
package storagemanager

import java.io.FileReader
import java.io.BufferedReader
import java.text.SimpleDateFormat

class K2DBScanner(filename: String) {
  var byteRead: Int = 0
  var intDigits: Int = 0
  var delimiter: Char = '|'
  var br: BufferedReader = new BufferedReader(new FileReader(filename))
  val sdf = new SimpleDateFormat("yyyy-MM-dd");

  def next_int() = {
    var number = 0
    var signed = false

    intDigits = 0
    byteRead = br.read()
    if (byteRead == '-') {
      signed = true
      byteRead = br.read()
    }
    while (Character.isDigit(byteRead)) {
      number *= 10
      number += byteRead - '0'
      byteRead = br.read()
      intDigits = intDigits + 1
    }
    if ((byteRead != delimiter) && (byteRead != '.') && (byteRead != '\n'))
      throw new RuntimeException("Tried to read Integer, but found neither delimiter nor . after number (found " +
        byteRead.asInstanceOf[Char] + ", previous token = " + intDigits + "/" + number + ")")
    if (signed) -1 * number else number
  }

  def next_double() = {
    val numeral: Double = next_int()
    var fractal: Double = 0.0
    // Has fractal part
    if (byteRead == '.') {
      fractal = next_int()
      while (intDigits > 0) {
        fractal = fractal * 0.1
        intDigits = intDigits - 1
      }
    }
    numeral + fractal
  }

  def next_char() = {
    byteRead = br.read()
    val del = br.read() //delimiter
    if ((del != delimiter) && (del != '\n'))
      throw new RuntimeException("Expected delimiter after char. Not found. Sorry!")
    byteRead.asInstanceOf[Char]
  }

  def next(buf: Array[Byte]): Int = {
    next(buf, 0)
  }

  def next(buf: Array[Byte], offset: Int) = {
    byteRead = br.read()
    var cnt = offset
    while (br.ready() && (byteRead != delimiter) && (byteRead != '\n')) {
      buf(cnt) = byteRead.asInstanceOf[Byte]
      byteRead = br.read()
      cnt += 1
    }
    cnt
  }

  def next_date: Int = {
    delimiter = '-'
    val year = next_int
    val month = next_int
    delimiter = '|'
    val day = next_int
    //val date_str = year + "-" + month + "-" + day
    year * 10000 + month * 100 + day
  }

  def hasNext() = {
    val f = br.ready()
    if (!f) br.close
    f
  }
}
