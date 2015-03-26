package ch.epfl.data
package legobase


import scala.collection.mutable.Set
import scala.collection.mutable.HashMap
import scala.collection.mutable.TreeSet
import scala.collection.mutable.ArrayBuffer
import storagemanager.K2DBScanner
import storagemanager.Loader
import queryengine.GenericEngine
import pardis.shallow.OptimalString
import pardis.shallow.scalalib.collection.Cont

class MultiMap[T, S] extends HashMap[T, Set[S]] with scala.collection.mutable.MultiMap[T, S]

object OrderingFactory {
  def apply[T](fun: (T, T) => Int): Ordering[T] = new Ordering[T] {
    def compare(o1: T, o2: T) = fun(o1, o2)
  }
}


case class REGIONRecord(val R_REGIONKEY: Int, val R_NAME: OptimalString, val R_COMMENT: OptimalString)
case class WindowRecord_Int_DynamicCompositeRecord_REGIONRecord_DynamicCompositeRecord_PARTRecord_DynamicCompositeRecord_NATIONRecord_DynamicCompositeRecord_SUPPLIERRecord_PARTSUPPRecord(val key: Int, val wnd: REGIONRecord_PARTRecord_NATIONRecord_SUPPLIERRecord_PARTSUPPRecord)
case class NATIONRecord(val N_NATIONKEY: Int, val N_NAME: OptimalString, val N_REGIONKEY: Int, val N_COMMENT: OptimalString)
case class REGIONRecord_PARTRecord_NATIONRecord_SUPPLIERRecord_PARTSUPPRecord(val R_REGIONKEY: Int, val R_NAME: OptimalString, val R_COMMENT: OptimalString, val P_PARTKEY: Int, val P_NAME: OptimalString, val P_MFGR: OptimalString, val P_BRAND: OptimalString, val P_TYPE: OptimalString, val P_SIZE: Int, val P_CONTAINER: OptimalString, val P_RETAILPRICE: Double, val P_COMMENT: OptimalString, val N_NATIONKEY: Int, val N_NAME: OptimalString, val N_REGIONKEY: Int, val N_COMMENT: OptimalString, val S_SUPPKEY: Int, val S_NAME: OptimalString, val S_ADDRESS: OptimalString, val S_NATIONKEY: Int, val S_PHONE: OptimalString, val S_ACCTBAL: Double, val S_COMMENT: OptimalString, val PS_PARTKEY: Int, val PS_SUPPKEY: Int, val PS_AVAILQTY: Int, val PS_SUPPLYCOST: Double, val PS_COMMENT: OptimalString)
case class PARTRecord(val P_PARTKEY: Int, val P_NAME: OptimalString, val P_MFGR: OptimalString, val P_BRAND: OptimalString, val P_TYPE: OptimalString, val P_SIZE: Int, val P_CONTAINER: OptimalString, val P_RETAILPRICE: Double, val P_COMMENT: OptimalString)
case class SUPPLIERRecord(val S_SUPPKEY: Int, val S_NAME: OptimalString, val S_ADDRESS: OptimalString, val S_NATIONKEY: Int, val S_PHONE: OptimalString, val S_ACCTBAL: Double, val S_COMMENT: OptimalString)
case class PARTSUPPRecord(val PS_PARTKEY: Int, val PS_SUPPKEY: Int, val PS_AVAILQTY: Int, val PS_SUPPLYCOST: Double, val PS_COMMENT: OptimalString)
object Q2 extends LegoRunner {
  def executeQuery(query: String, sf: Double): Unit = main()
  def main(args: Array[String]) {
    run(args)
  }
  def main() = 
  {
    val x1 = Loader.fileLineCount("/home/florian/Documents/tpch_testdata/sf0.1/part.tbl")
    val x2 = new K2DBScanner("/home/florian/Documents/tpch_testdata/sf0.1/part.tbl")
    val x3 = new Array[PARTRecord](x1)
    var i4: Int = 0
    val x61 = while({
      val x5 = x2.hasNext()
      x5
    })
    {
      val x6 = x2.next_int()
      val x8 = new Array[Byte](56)
      val x9 = x2.next(x8)
      val x12 = { x10: Byte => {
          val x11 = x10.!=(0)
          x11
        }
      }
      val x13 = x8.filter(x12)
      val x14 = new OptimalString(x13)
      val x16 = new Array[Byte](26)
      val x17 = x2.next(x16)
      val x20 = { x18: Byte => {
          val x19 = x18.!=(0)
          x19
        }
      }
      val x21 = x16.filter(x20)
      val x22 = new OptimalString(x21)
      val x24 = new Array[Byte](11)
      val x25 = x2.next(x24)
      val x28 = { x26: Byte => {
          val x27 = x26.!=(0)
          x27
        }
      }
      val x29 = x24.filter(x28)
      val x30 = new OptimalString(x29)
      val x31 = new Array[Byte](26)
      val x32 = x2.next(x31)
      val x35 = { x33: Byte => {
          val x34 = x33.!=(0)
          x34
        }
      }
      val x36 = x31.filter(x35)
      val x37 = new OptimalString(x36)
      val x38 = x2.next_int()
      val x39 = new Array[Byte](11)
      val x40 = x2.next(x39)
      val x43 = { x41: Byte => {
          val x42 = x41.!=(0)
          x42
        }
      }
      val x44 = x39.filter(x43)
      val x45 = new OptimalString(x44)
      val x46 = x2.next_double()
      val x48 = new Array[Byte](24)
      val x49 = x2.next(x48)
      val x52 = { x50: Byte => {
          val x51 = x50.!=(0)
          x51
        }
      }
      val x53 = x48.filter(x52)
      val x54 = new OptimalString(x53)
      val x55 = PARTRecord(x6, x14, x22, x30, x37, x38, x45, x46, x54)
      val x56 = i4
      val x57 = x3.update(x56, x55)
      val x58 = i4
      val x59 = x58.+(1)
      val x60 = i4 = x59
      x60
    }
    val x62 = Loader.fileLineCount("/home/florian/Documents/tpch_testdata/sf0.1/partsupp.tbl")
    val x63 = new K2DBScanner("/home/florian/Documents/tpch_testdata/sf0.1/partsupp.tbl")
    val x64 = new Array[PARTSUPPRecord](x62)
    var i65: Int = 0
    val x85 = while({
      val x66 = x63.hasNext()
      x66
    })
    {
      val x67 = x63.next_int()
      val x68 = x63.next_int()
      val x69 = x63.next_int()
      val x70 = x63.next_double()
      val x72 = new Array[Byte](200)
      val x73 = x63.next(x72)
      val x76 = { x74: Byte => {
          val x75 = x74.!=(0)
          x75
        }
      }
      val x77 = x72.filter(x76)
      val x78 = new OptimalString(x77)
      val x79 = PARTSUPPRecord(x67, x68, x69, x70, x78)
      val x80 = i65
      val x81 = x64.update(x80, x79)
      val x82 = i65
      val x83 = x82.+(1)
      val x84 = i65 = x83
      x84
    }
    val x86 = Loader.fileLineCount("/home/florian/Documents/tpch_testdata/sf0.1/nation.tbl")
    val x87 = new K2DBScanner("/home/florian/Documents/tpch_testdata/sf0.1/nation.tbl")
    val x88 = new Array[NATIONRecord](x86)
    var i89: Int = 0
    val x115 = while({
      val x90 = x87.hasNext()
      x90
    })
    {
      val x91 = x87.next_int()
      val x93 = new Array[Byte](26)
      val x94 = x87.next(x93)
      val x97 = { x95: Byte => {
          val x96 = x95.!=(0)
          x96
        }
      }
      val x98 = x93.filter(x97)
      val x99 = new OptimalString(x98)
      val x100 = x87.next_int()
      val x102 = new Array[Byte](153)
      val x103 = x87.next(x102)
      val x106 = { x104: Byte => {
          val x105 = x104.!=(0)
          x105
        }
      }
      val x107 = x102.filter(x106)
      val x108 = new OptimalString(x107)
      val x109 = NATIONRecord(x91, x99, x100, x108)
      val x110 = i89
      val x111 = x88.update(x110, x109)
      val x112 = i89
      val x113 = x112.+(1)
      val x114 = i89 = x113
      x114
    }
    val x116 = Loader.fileLineCount("/home/florian/Documents/tpch_testdata/sf0.1/region.tbl")
    val x117 = new K2DBScanner("/home/florian/Documents/tpch_testdata/sf0.1/region.tbl")
    val x118 = new Array[REGIONRecord](x116)
    var i119: Int = 0
    val x144 = while({
      val x120 = x117.hasNext()
      x120
    })
    {
      val x121 = x117.next_int()
      val x123 = new Array[Byte](26)
      val x124 = x117.next(x123)
      val x127 = { x125: Byte => {
          val x126 = x125.!=(0)
          x126
        }
      }
      val x128 = x123.filter(x127)
      val x129 = new OptimalString(x128)
      val x131 = new Array[Byte](153)
      val x132 = x117.next(x131)
      val x135 = { x133: Byte => {
          val x134 = x133.!=(0)
          x134
        }
      }
      val x136 = x131.filter(x135)
      val x137 = new OptimalString(x136)
      val x138 = REGIONRecord(x121, x129, x137)
      val x139 = i119
      val x140 = x118.update(x139, x138)
      val x141 = i119
      val x142 = x141.+(1)
      val x143 = i119 = x142
      x143
    }
    val x145 = Loader.fileLineCount("/home/florian/Documents/tpch_testdata/sf0.1/supplier.tbl")
    val x146 = new K2DBScanner("/home/florian/Documents/tpch_testdata/sf0.1/supplier.tbl")
    val x147 = new Array[SUPPLIERRecord](x145)
    var i148: Int = 0
    val x191 = while({
      val x149 = x146.hasNext()
      x149
    })
    {
      val x150 = x146.next_int()
      val x152 = new Array[Byte](26)
      val x153 = x146.next(x152)
      val x156 = { x154: Byte => {
          val x155 = x154.!=(0)
          x155
        }
      }
      val x157 = x152.filter(x156)
      val x158 = new OptimalString(x157)
      val x160 = new Array[Byte](41)
      val x161 = x146.next(x160)
      val x164 = { x162: Byte => {
          val x163 = x162.!=(0)
          x163
        }
      }
      val x165 = x160.filter(x164)
      val x166 = new OptimalString(x165)
      val x167 = x146.next_int()
      val x169 = new Array[Byte](16)
      val x170 = x146.next(x169)
      val x173 = { x171: Byte => {
          val x172 = x171.!=(0)
          x172
        }
      }
      val x174 = x169.filter(x173)
      val x175 = new OptimalString(x174)
      val x176 = x146.next_double()
      val x178 = new Array[Byte](102)
      val x179 = x146.next(x178)
      val x182 = { x180: Byte => {
          val x181 = x180.!=(0)
          x181
        }
      }
      val x183 = x178.filter(x182)
      val x184 = new OptimalString(x183)
      val x185 = SUPPLIERRecord(x150, x158, x166, x167, x175, x176, x184)
      val x186 = i148
      val x187 = x147.update(x186, x185)
      val x188 = i148
      val x189 = x188.+(1)
      val x190 = i148 = x189
      x190
    }
    val x192 = new Range(0, 1, 1)
    val x848 = { x193: Int => {
        val x1174 = new MultiMap[Int, REGIONRecord_PARTRecord_NATIONRecord_SUPPLIERRecord_PARTSUPPRecord]()
        val x1159 = new MultiMap[Int, REGIONRecord]()
        val x1128 = new MultiMap[Int, PARTRecord]()
        val x1094 = new MultiMap[Int, NATIONRecord]()
        val x1071 = new MultiMap[Int, SUPPLIERRecord]()
        val x847 = GenericEngine.runQuery({
          val x194 = GenericEngine.parseString("AFRICA")
          val x195 = GenericEngine.parseString("TIN")
          var x1048: Int = 0
          var x1053: Int = 0
          var x1076: Int = 0
          var x1099: Int = 0
          var x1133: Int = 0
          val x305 = { (x276: WindowRecord_Int_DynamicCompositeRecord_REGIONRecord_DynamicCompositeRecord_PARTRecord_DynamicCompositeRecord_NATIONRecord_DynamicCompositeRecord_SUPPLIERRecord_PARTSUPPRecord, x277: WindowRecord_Int_DynamicCompositeRecord_REGIONRecord_DynamicCompositeRecord_PARTRecord_DynamicCompositeRecord_NATIONRecord_DynamicCompositeRecord_SUPPLIERRecord_PARTSUPPRecord) => {
              val x278 = x276.wnd
              val x279 = x278.S_ACCTBAL
              val x280 = x277.wnd
              val x281 = x280.S_ACCTBAL
              val x282 = x279.<(x281)
              val x304 = if(x282) 
              {
                1
              }
              else
              {
                val x283 = x279.>(x281)
                val x303 = if(x283) 
                {
                  -1
                }
                else
                {
                  val x284 = x278.N_NAME
                  val x285 = x280.N_NAME
                  val x286 = x284.diff(x285)
                  var res287: Int = x286
                  val x288 = res287
                  val x289 = x288.==(0)
                  val x301 = if(x289) 
                  {
                    val x290 = x278.S_NAME
                    val x291 = x280.S_NAME
                    val x292 = x290.diff(x291)
                    val x293 = res287 = x292
                    val x294 = res287
                    val x295 = x294.==(0)
                    val x300 = if(x295) 
                    {
                      val x296 = x278.P_PARTKEY
                      val x297 = x280.P_PARTKEY
                      val x298 = x296.-(x297)
                      val x299 = res287 = x298
                      x299
                    }
                    else
                    {
                      ()
                    }
                    
                    x300
                  }
                  else
                  {
                    ()
                  }
                  
                  val x302 = res287
                  x302
                }
                
                x303
              }
              
              x304
            }
          }
          val x1209 = OrderingFactory(x305)
          val x1210 = new TreeSet()(x1209)
          var x1211: Boolean = false
          var j307: Int = 0
          var x1240: Int = 0
          val x350 = while({
            val x337 = true.&&({
              val x4410 = x1133
              val x336 = x4410.<(x116)
              x336
            })
            x337
          })
          {
            val x4412 = x1133
            val x339 = x118.apply(x4412)
            val x341 = x339.R_NAME
            val x342 = x341.===(x194)
            val x346 = if(x342) 
            {
              val x343 = x339.R_REGIONKEY
              val x345 = x1159.addBinding(x343, x339)
              ()
            }
            else
            {
              ()
            }
            
            val x4420 = x1133
            val x348 = x4420.+(1)
            val x4422 = x1133 = x348
            x4422
          }
          val x371 = while({
            val x355 = true.&&({
              val x4427 = x1099
              val x354 = x4427.<(x1)
              x354
            })
            x355
          })
          {
            val x4429 = x1099
            val x357 = x3.apply(x4429)
            val x359 = x357.P_SIZE
            val x360 = x359.==(43)
            val x363 = x360.&&({
              val x361 = x357.P_TYPE
              val x362 = x361.endsWith(x195)
              x362
            })
            val x367 = if(x363) 
            {
              val x364 = x357.P_PARTKEY
              val x366 = x1128.addBinding(x364, x357)
              ()
            }
            else
            {
              ()
            }
            
            val x4440 = x1099
            val x369 = x4440.+(1)
            val x4442 = x1099 = x369
            x4442
          }
          val x386 = while({
            val x376 = true.&&({
              val x4447 = x1076
              val x375 = x4447.<(x86)
              x375
            })
            x376
          })
          {
            val x4449 = x1076
            val x378 = x88.apply(x4449)
            val x380 = x378.N_NATIONKEY
            val x382 = x1094.addBinding(x380, x378)
            val x4454 = x1076
            val x384 = x4454.+(1)
            val x4456 = x1076 = x384
            x4456
          }
          val x401 = while({
            val x391 = true.&&({
              val x4461 = x1053
              val x390 = x4461.<(x145)
              x390
            })
            x391
          })
          {
            val x4463 = x1053
            val x393 = x147.apply(x4463)
            val x395 = x393.S_SUPPKEY
            val x397 = x1071.addBinding(x395, x393)
            val x4468 = x1053
            val x399 = x4468.+(1)
            val x4470 = x1053 = x399
            x4470
          }
          val x791 = while({
            val x406 = true.&&({
              val x4475 = x1048
              val x405 = x4475.<(x62)
              x405
            })
            x406
          })
          {
            val x4477 = x1048
            val x408 = x64.apply(x4477)
            val x410 = x408.PS_SUPPKEY
            val x412 = x1071.get(x410)
            val x600 = x412.nonEmpty
            val x787 = if(x600) 
            {
              val x601 = x412.get
              val x785 = { x602: SUPPLIERRecord => {
                  val x603 = x602.S_SUPPKEY
                  val x604 = x603.==(x410)
                  val x784 = if(x604) 
                  {
                    val x1859 = x602.S_NAME
                    val x1860 = x602.S_ADDRESS
                    val x1861 = x602.S_NATIONKEY
                    val x1862 = x602.S_PHONE
                    val x1863 = x602.S_ACCTBAL
                    val x1864 = x602.S_COMMENT
                    val x1865 = x408.PS_PARTKEY
                    val x1866 = x408.PS_AVAILQTY
                    val x1867 = x408.PS_SUPPLYCOST
                    val x1868 = x408.PS_COMMENT
                    val x608 = x1094.get(x1861)
                    val x696 = x608.nonEmpty
                    val x783 = if(x696) 
                    {
                      val x697 = x608.get
                      val x781 = { x698: NATIONRecord => {
                          val x699 = x698.N_NATIONKEY
                          val x700 = x699.==(x1861)
                          val x780 = if(x700) 
                          {
                            val x2132 = x698.N_NAME
                            val x2133 = x698.N_REGIONKEY
                            val x2134 = x698.N_COMMENT
                            val x704 = x1128.get(x1865)
                            val x742 = x704.nonEmpty
                            val x779 = if(x742) 
                            {
                              val x743 = x704.get
                              val x777 = { x744: PARTRecord => {
                                  val x745 = x744.P_PARTKEY
                                  val x746 = x745.==(x1865)
                                  val x776 = if(x746) 
                                  {
                                    val x2270 = x744.P_NAME
                                    val x2271 = x744.P_MFGR
                                    val x2272 = x744.P_BRAND
                                    val x2273 = x744.P_TYPE
                                    val x2274 = x744.P_SIZE
                                    val x2275 = x744.P_CONTAINER
                                    val x2276 = x744.P_RETAILPRICE
                                    val x2277 = x744.P_COMMENT
                                    val x750 = x1159.get(x2133)
                                    val x763 = x750.nonEmpty
                                    val x775 = if(x763) 
                                    {
                                      val x764 = x750.get
                                      val x773 = { x765: REGIONRecord => {
                                          val x766 = x765.R_REGIONKEY
                                          val x767 = x766.==(x2133)
                                          val x772 = if(x767) 
                                          {
                                            val x2343 = x765.R_NAME
                                            val x2344 = x765.R_COMMENT
                                            val x768 = REGIONRecord_PARTRecord_NATIONRecord_SUPPLIERRecord_PARTSUPPRecord(x766, x2343, x2344, x745, x2270, x2271, x2272, x2273, x2274, x2275, x2276, x2277, x699, x2132, x2133, x2134, x603, x1859, x1860, x1861, x1862, x1863, x1864, x1865, x410, x1866, x1867, x1868)
                                            val x769 = x768.P_PARTKEY
                                            val x771 = x1174.addBinding(x769, x768)
                                            ()
                                          }
                                          else
                                          {
                                            ()
                                          }
                                          
                                          x772
                                        }
                                      }
                                      val x774 = x764.foreach(x773)
                                      ()
                                    }
                                    else
                                    {
                                      ()
                                    }
                                    
                                    x775
                                  }
                                  else
                                  {
                                    ()
                                  }
                                  
                                  x776
                                }
                              }
                              val x778 = x743.foreach(x777)
                              ()
                            }
                            else
                            {
                              ()
                            }
                            
                            x779
                          }
                          else
                          {
                            ()
                          }
                          
                          x780
                        }
                      }
                      val x782 = x697.foreach(x781)
                      ()
                    }
                    else
                    {
                      ()
                    }
                    
                    x783
                  }
                  else
                  {
                    ()
                  }
                  
                  x784
                }
              }
              val x786 = x601.foreach(x785)
              ()
            }
            else
            {
              ()
            }
            
            val x4941 = x1048
            val x789 = x4941.+(1)
            val x4943 = x1048 = x789
            x4943
          }
          val x805 = { x794: Tuple2[Int, Set[REGIONRecord_PARTRecord_NATIONRecord_SUPPLIERRecord_PARTSUPPRecord]] => {
              val x795 = x794._2
              val x798 = { x796: REGIONRecord_PARTRecord_NATIONRecord_SUPPLIERRecord_PARTSUPPRecord => {
                  val x797 = x796.PS_SUPPLYCOST
                  x797
                }
              }
              val x799 = x795.minBy(x798)
              val x800 = x795.head
              val x801 = x800.P_PARTKEY
              val x802 = WindowRecord_Int_DynamicCompositeRecord_REGIONRecord_DynamicCompositeRecord_PARTRecord_DynamicCompositeRecord_NATIONRecord_DynamicCompositeRecord_SUPPLIERRecord_PARTSUPPRecord(x801, x799)
              val x804 = x1210.+=(x802)
              ()
            }
          }
          val x806 = x1174.foreach(x805)
          val x844 = while({
            val x4958 = x1211
            val x808 = x4958.unary_!
            val x812 = x808.&&({
              val x810 = x1210.size
              val x811 = x810.!=(0)
              x811
            })
            x812
          })
          {
            val x814 = x1210.head
            val x815 = x1210.-=(x814)
            val x817 = j307
            val x818 = x817.<(100)
            val x819 = x818.==(false)
            val x843 = if(x819) 
            {
              val x4970 = x1211 = true
              x4970
            }
            else
            {
              val x821 = x814.wnd
              val x822 = x821.S_ACCTBAL
              val x823 = x821.S_NAME
              val x824 = x823.string
              val x825 = x821.N_NAME
              val x826 = x825.string
              val x827 = x821.P_PARTKEY
              val x828 = x821.P_MFGR
              val x829 = x828.string
              val x830 = x821.S_ADDRESS
              val x831 = x830.string
              val x832 = x821.S_PHONE
              val x833 = x832.string
              val x834 = x821.S_COMMENT
              val x835 = x834.string
              val x836 = printf("%.2f|%s|%s|%d|%s|%s|%s|%s\n", x822, x824, x826, x827, x829, x831, x833, x835)
              val x837 = j307
              val x838 = x837.+(1)
              val x839 = j307 = x838
              val x4990 = x1240
              val x841 = x4990.+(1)
              val x4992 = x1240 = x841
              x4992
            }
            
            x843
          }
          val x4993 = x1240
          val x846 = printf("(%d rows)\n", x4993)
          ()
        })
        x847
      }
    }
    val x849 = x192.foreach(x848)
    x849
  }
}