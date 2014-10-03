
package ch.epfl.data
package legobase
package deep

import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.deep.scalalib._
import pardis.deep.scalalib.collection._
trait LoaderOps extends Base { this: DeepDSL =>
  // Type representation
  case object LoaderType extends TypeRep[Loader] {
    def rebuild(newArguments: TypeRep[_]*): TypeRep[_] = LoaderType
    val name = "Loader"
    val typeArguments = Nil

    val typeTag = scala.reflect.runtime.universe.typeTag[Loader]
  }
  implicit val typeLoader = LoaderType
  implicit class LoaderRep(self: Rep[Loader]) {

  }
  object Loader {
    def getFullPath(fileName: Rep[String]): Rep[String] = loaderGetFullPathObject(fileName)
    def loadString(size: Rep[Int], s: Rep[K2DBScanner]): Rep[OptimalString] = loaderLoadStringObject(size, s)
    def fileLineCount(file: Rep[String]): Rep[Int] = loaderFileLineCountObject(file)
    def loadRegion(): Rep[Array[REGIONRecord]] = loaderLoadRegionObject()
    def loadPartsupp(): Rep[Array[PARTSUPPRecord]] = loaderLoadPartsuppObject()
    def loadPart(): Rep[Array[PARTRecord]] = loaderLoadPartObject()
    def loadNation(): Rep[Array[NATIONRecord]] = loaderLoadNationObject()
    def loadSupplier(): Rep[Array[SUPPLIERRecord]] = loaderLoadSupplierObject()
    def loadLineitem(): Rep[Array[LINEITEMRecord]] = loaderLoadLineitemObject()
    def loadOrders(): Rep[Array[ORDERSRecord]] = loaderLoadOrdersObject()
    def loadCustomer(): Rep[Array[CUSTOMERRecord]] = loaderLoadCustomerObject()
  }
  // constructors

  // case classes
  case class LoaderGetFullPathObject(fileName: Rep[String]) extends FunctionDef[String](None, "Loader.getFullPath", List(List(fileName))) {
    override def curriedConstructor = (copy _)
  }

  case class LoaderLoadStringObject(size: Rep[Int], s: Rep[K2DBScanner]) extends FunctionDef[OptimalString](None, "Loader.loadString", List(List(size, s))) {
    override def curriedConstructor = (copy _).curried
  }

  case class LoaderFileLineCountObject(file: Rep[String]) extends FunctionDef[Int](None, "Loader.fileLineCount", List(List(file))) {
    override def curriedConstructor = (copy _)
  }

  case class LoaderLoadRegionObject() extends FunctionDef[Array[REGIONRecord]](None, "Loader.loadRegion", List(List())) {
    override def curriedConstructor = (x: Any) => copy()
  }

  case class LoaderLoadPartsuppObject() extends FunctionDef[Array[PARTSUPPRecord]](None, "Loader.loadPartsupp", List(List())) {
    override def curriedConstructor = (x: Any) => copy()
  }

  case class LoaderLoadPartObject() extends FunctionDef[Array[PARTRecord]](None, "Loader.loadPart", List(List())) {
    override def curriedConstructor = (x: Any) => copy()
  }

  case class LoaderLoadNationObject() extends FunctionDef[Array[NATIONRecord]](None, "Loader.loadNation", List(List())) {
    override def curriedConstructor = (x: Any) => copy()
  }

  case class LoaderLoadSupplierObject() extends FunctionDef[Array[SUPPLIERRecord]](None, "Loader.loadSupplier", List(List())) {
    override def curriedConstructor = (x: Any) => copy()
  }

  case class LoaderLoadLineitemObject() extends FunctionDef[Array[LINEITEMRecord]](None, "Loader.loadLineitem", List(List())) {
    override def curriedConstructor = (x: Any) => copy()
  }

  case class LoaderLoadOrdersObject() extends FunctionDef[Array[ORDERSRecord]](None, "Loader.loadOrders", List(List())) {
    override def curriedConstructor = (x: Any) => copy()
  }

  case class LoaderLoadCustomerObject() extends FunctionDef[Array[CUSTOMERRecord]](None, "Loader.loadCustomer", List(List())) {
    override def curriedConstructor = (x: Any) => copy()
  }

  // method definitions
  def loaderGetFullPathObject(fileName: Rep[String]): Rep[String] = LoaderGetFullPathObject(fileName)
  def loaderLoadStringObject(size: Rep[Int], s: Rep[K2DBScanner]): Rep[OptimalString] = LoaderLoadStringObject(size, s)
  def loaderFileLineCountObject(file: Rep[String]): Rep[Int] = LoaderFileLineCountObject(file)
  def loaderLoadRegionObject(): Rep[Array[REGIONRecord]] = LoaderLoadRegionObject()
  def loaderLoadPartsuppObject(): Rep[Array[PARTSUPPRecord]] = LoaderLoadPartsuppObject()
  def loaderLoadPartObject(): Rep[Array[PARTRecord]] = LoaderLoadPartObject()
  def loaderLoadNationObject(): Rep[Array[NATIONRecord]] = LoaderLoadNationObject()
  def loaderLoadSupplierObject(): Rep[Array[SUPPLIERRecord]] = LoaderLoadSupplierObject()
  def loaderLoadLineitemObject(): Rep[Array[LINEITEMRecord]] = LoaderLoadLineitemObject()
  def loaderLoadOrdersObject(): Rep[Array[ORDERSRecord]] = LoaderLoadOrdersObject()
  def loaderLoadCustomerObject(): Rep[Array[CUSTOMERRecord]] = LoaderLoadCustomerObject()
  type Loader = ch.epfl.data.legobase.storagemanager.Loader
}
trait LoaderImplicits { this: DeepDSL =>
  // Add implicit conversions here!
}
trait LoaderImplementations { this: DeepDSL =>
  override def loaderLoadStringObject(size: Rep[Int], s: Rep[K2DBScanner]): Rep[OptimalString] = {
    {
      val NAME: this.Rep[Array[Byte]] = __newArray[Byte](size);
      s.next(NAME);
      OptimalString.apply(byteArrayOps(NAME).filter(__lambda(((y: this.Rep[Byte]) => infix_$bang$eq(y, unit(0))))))
    }
  }
  override def loaderLoadRegionObject(): Rep[Array[REGIONRecord]] = {
    {
      val file: this.Rep[String] = Loader.getFullPath(unit("region.tbl"));
      val size: this.Rep[Int] = Loader.fileLineCount(file);
      val s: this.Rep[ch.epfl.data.legobase.storagemanager.K2DBScanner] = __newK2DBScanner(file);
      val hm: this.Rep[Array[ch.epfl.data.legobase.queryengine.TPCHRelations.REGIONRecord]] = __newArray[ch.epfl.data.legobase.queryengine.TPCHRelations.REGIONRecord](size);
      var i: this.Var[Int] = __newVar(unit(0));
      __whileDo(s.hasNext(), {
        val newEntry: this.Rep[ch.epfl.data.legobase.queryengine.TPCHRelations.REGIONRecord] = __newREGIONRecord(s.next_int(), Loader.loadString(unit(25), s), Loader.loadString(unit(152), s));
        hm.update(__readVar(i), newEntry);
        __assign(i, __readVar(i).$plus(unit(1)))
      });
      hm
    }
  }
  override def loaderLoadPartsuppObject(): Rep[Array[PARTSUPPRecord]] = {
    {
      val file: this.Rep[String] = Loader.getFullPath(unit("partsupp.tbl"));
      val size: this.Rep[Int] = Loader.fileLineCount(file);
      val s: this.Rep[ch.epfl.data.legobase.storagemanager.K2DBScanner] = __newK2DBScanner(file);
      val hm: this.Rep[Array[ch.epfl.data.legobase.queryengine.TPCHRelations.PARTSUPPRecord]] = __newArray[ch.epfl.data.legobase.queryengine.TPCHRelations.PARTSUPPRecord](size);
      var i: this.Var[Int] = __newVar(unit(0));
      __whileDo(s.hasNext(), {
        val newEntry: this.Rep[ch.epfl.data.legobase.queryengine.TPCHRelations.PARTSUPPRecord] = __newPARTSUPPRecord(s.next_int(), s.next_int(), s.next_int(), s.next_double(), Loader.loadString(unit(199), s));
        hm.update(__readVar(i), newEntry);
        __assign(i, __readVar(i).$plus(unit(1)))
      });
      hm
    }
  }
  override def loaderLoadPartObject(): Rep[Array[PARTRecord]] = {
    {
      val file: this.Rep[String] = Loader.getFullPath(unit("part.tbl"));
      val size: this.Rep[Int] = Loader.fileLineCount(file);
      val s: this.Rep[ch.epfl.data.legobase.storagemanager.K2DBScanner] = __newK2DBScanner(file);
      val hm: this.Rep[Array[ch.epfl.data.legobase.queryengine.TPCHRelations.PARTRecord]] = __newArray[ch.epfl.data.legobase.queryengine.TPCHRelations.PARTRecord](size);
      var i: this.Var[Int] = __newVar(unit(0));
      __whileDo(s.hasNext(), {
        val newEntry: this.Rep[ch.epfl.data.legobase.queryengine.TPCHRelations.PARTRecord] = __newPARTRecord(s.next_int(), Loader.loadString(unit(55), s), Loader.loadString(unit(25), s), Loader.loadString(unit(10), s), Loader.loadString(unit(25), s), s.next_int(), Loader.loadString(unit(10), s), s.next_double(), Loader.loadString(unit(23), s));
        hm.update(__readVar(i), newEntry);
        __assign(i, __readVar(i).$plus(unit(1)))
      });
      hm
    }
  }
  override def loaderLoadNationObject(): Rep[Array[NATIONRecord]] = {
    {
      val file: this.Rep[String] = Loader.getFullPath(unit("nation.tbl"));
      val size: this.Rep[Int] = Loader.fileLineCount(file);
      val s: this.Rep[ch.epfl.data.legobase.storagemanager.K2DBScanner] = __newK2DBScanner(file);
      val hm: this.Rep[Array[ch.epfl.data.legobase.queryengine.TPCHRelations.NATIONRecord]] = __newArray[ch.epfl.data.legobase.queryengine.TPCHRelations.NATIONRecord](size);
      var i: this.Var[Int] = __newVar(unit(0));
      __whileDo(s.hasNext(), {
        val newEntry: this.Rep[ch.epfl.data.legobase.queryengine.TPCHRelations.NATIONRecord] = __newNATIONRecord(s.next_int(), Loader.loadString(unit(25), s), s.next_int(), Loader.loadString(unit(152), s));
        hm.update(__readVar(i), newEntry);
        __assign(i, __readVar(i).$plus(unit(1)))
      });
      hm
    }
  }
  override def loaderLoadSupplierObject(): Rep[Array[SUPPLIERRecord]] = {
    {
      val file: this.Rep[String] = Loader.getFullPath(unit("supplier.tbl"));
      val size: this.Rep[Int] = Loader.fileLineCount(file);
      val s: this.Rep[ch.epfl.data.legobase.storagemanager.K2DBScanner] = __newK2DBScanner(file);
      val hm: this.Rep[Array[ch.epfl.data.legobase.queryengine.TPCHRelations.SUPPLIERRecord]] = __newArray[ch.epfl.data.legobase.queryengine.TPCHRelations.SUPPLIERRecord](size);
      var i: this.Var[Int] = __newVar(unit(0));
      __whileDo(s.hasNext(), {
        val newEntry: this.Rep[ch.epfl.data.legobase.queryengine.TPCHRelations.SUPPLIERRecord] = __newSUPPLIERRecord(s.next_int(), Loader.loadString(unit(25), s), Loader.loadString(unit(40), s), s.next_int(), Loader.loadString(unit(15), s), s.next_double(), Loader.loadString(unit(101), s));
        hm.update(__readVar(i), newEntry);
        __assign(i, __readVar(i).$plus(unit(1)))
      });
      hm
    }
  }
  override def loaderLoadLineitemObject(): Rep[Array[LINEITEMRecord]] = {
    {
      val file: this.Rep[String] = Loader.getFullPath(unit("lineitem.tbl"));
      val size: this.Rep[Int] = Loader.fileLineCount(file);
      val s: this.Rep[ch.epfl.data.legobase.storagemanager.K2DBScanner] = __newK2DBScanner(file);
      val hm: this.Rep[Array[ch.epfl.data.legobase.queryengine.TPCHRelations.LINEITEMRecord]] = __newArray[ch.epfl.data.legobase.queryengine.TPCHRelations.LINEITEMRecord](size);
      var i: this.Var[Int] = __newVar(unit(0));
      __whileDo(s.hasNext(), {
        val newEntry: this.Rep[ch.epfl.data.legobase.queryengine.TPCHRelations.LINEITEMRecord] = __newLINEITEMRecord(s.next_int(), s.next_int(), s.next_int(), s.next_int(), s.next_double(), s.next_double(), s.next_double(), s.next_double(), s.next_char(), s.next_char(), s.next_date, s.next_date, s.next_date, Loader.loadString(unit(25), s), Loader.loadString(unit(10), s), Loader.loadString(unit(44), s));
        hm.update(__readVar(i), newEntry);
        __assign(i, __readVar(i).$plus(unit(1)))
      });
      hm
    }
  }
  override def loaderLoadOrdersObject(): Rep[Array[ORDERSRecord]] = {
    {
      val file: this.Rep[String] = Loader.getFullPath(unit("orders.tbl"));
      val size: this.Rep[Int] = Loader.fileLineCount(file);
      val s: this.Rep[ch.epfl.data.legobase.storagemanager.K2DBScanner] = __newK2DBScanner(file);
      val hm: this.Rep[Array[ch.epfl.data.legobase.queryengine.TPCHRelations.ORDERSRecord]] = __newArray[ch.epfl.data.legobase.queryengine.TPCHRelations.ORDERSRecord](size);
      var i: this.Var[Int] = __newVar(unit(0));
      __whileDo(s.hasNext(), {
        val newEntry: this.Rep[ch.epfl.data.legobase.queryengine.TPCHRelations.ORDERSRecord] = __newORDERSRecord(s.next_int(), s.next_int(), s.next_char(), s.next_double(), s.next_date, Loader.loadString(unit(15), s), Loader.loadString(unit(15), s), s.next_int(), Loader.loadString(unit(79), s));
        hm.update(__readVar(i), newEntry);
        __assign(i, __readVar(i).$plus(unit(1)))
      });
      hm
    }
  }
  override def loaderLoadCustomerObject(): Rep[Array[CUSTOMERRecord]] = {
    {
      val file: this.Rep[String] = Loader.getFullPath(unit("customer.tbl"));
      val size: this.Rep[Int] = Loader.fileLineCount(file);
      val s: this.Rep[ch.epfl.data.legobase.storagemanager.K2DBScanner] = __newK2DBScanner(file);
      val hm: this.Rep[Array[ch.epfl.data.legobase.queryengine.TPCHRelations.CUSTOMERRecord]] = __newArray[ch.epfl.data.legobase.queryengine.TPCHRelations.CUSTOMERRecord](size);
      var i: this.Var[Int] = __newVar(unit(0));
      __whileDo(s.hasNext(), {
        val newEntry: this.Rep[ch.epfl.data.legobase.queryengine.TPCHRelations.CUSTOMERRecord] = __newCUSTOMERRecord(s.next_int(), Loader.loadString(unit(25), s), Loader.loadString(unit(40), s), s.next_int(), Loader.loadString(unit(15), s), s.next_double(), Loader.loadString(unit(10), s), Loader.loadString(unit(117), s));
        hm.update(__readVar(i), newEntry);
        __assign(i, __readVar(i).$plus(unit(1)))
      });
      hm
    }
  }
}
trait LoaderComponent extends LoaderOps with LoaderImplicits { this: DeepDSL => }
trait LoadersComponent extends LoaderComponent { self: DeepDSL => }