package ch.epfl.data
package dblab.legobase
package compiler

import sc.pardis.language._
import deep.{ QMonadLanguage, MCHLanguage, MCLanguage }

/**
 * Handles the setting parameters which are passed as the main arguments of the program.
 */
class Settings(val args: List[String]) {

  /**
   * Produces an optimal combination of optimization flags
   */
  var optimalArgsHandler: ((String) => List[String]) = _

  def validate(): Settings = {
    for (arg <- args.filter(a => a.startsWith("+") || a.startsWith("-")).filter(arg => !Settings.ALL_SETTINGS.exists(_.matches(arg)))) {
      System.out.println(s"${Console.YELLOW}Warning${Console.RESET}: flag $arg is not defined!")
    }
    if (!hashMapLowering && (setToArray || setToLinkedList || containerFlattenning))
      throw new Exception("It's impossible to lower Sets without lowering HashMap and MultiMap!")
    if (hashMapLowering && hashMapNoCollision)
      throw new Exception(s"${HashMapNoCollisionSetting.flagName} and ${HashMapToSetSetting.flagName} cannot be chained together.")
    if (hasSetting(LargeOutputHoistingSetting) && targetLanguage != CCoreLanguage) {
      throw new Exception(s"${LargeOutputHoistingSetting.flagName} is only available for C Code Generation.")
    }
    if (pointerStore && oldCArrayHandling) {
      throw new Exception(s"${PointerStoreSetting.flagName} and ${CArrayAsStructSetting.flagName} cannot be chained together.")
    }
    if (chooseOptimal || chooseCompliant) {
      if (optimalArgsHandler == null)
        throw new Exception(s"${OptimalSetting.flagName} cannot be used for it, because there is no optimal handler defined for it.")
      val propName =
        if (chooseCompliant)
          "config/compliant.properties"
        else
          "config/optimal.properties"
      val newArgs = optimalArgsHandler(propName)
      // TODO rewrite using OptimizationLevelSetting
      val LEVELS_PREFIX = "-levels="
      val levels = args.find(a => a.startsWith(LEVELS_PREFIX)).map(_.substring(LEVELS_PREFIX.length).toInt).getOrElse(4)
      def available(setting: OptimizationSetting): Boolean = {
        val langLevel = setting.language match {
          case ScalaCoreLanguage              => 2
          case CCoreLanguage | QMonadLanguage => 1
          case MCHLanguage                    => 3
          case MCLanguage                     => 4
        }
        langLevel <= levels
      }
      val filteredArgs = newArgs.map(a =>
        a -> Settings.ALL_SETTINGS.find(_.matches(a)).get.asInstanceOf[OptimizationSetting]).filter(a => available(a._2)).map(_._1)
      val levelSpecificArgs =
        if (levels == 1) {
          (PointerStoreSetting :: NoSingletonHashMapSetting :: NoFieldRemovalSetting :: Nil).map(_.fullFlagName) ++ filteredArgs
        } else {
          filteredArgs
        }
      System.out.println(s"${Console.GREEN}Info${Console.RESET}: the arguments `${levelSpecificArgs.mkString(" ")}` used!")
      new Settings(args.filter(a => !OptimalSetting.matches(a) && !CompliantSetting.matches(a)) ++ levelSpecificArgs).validate()
    } else {
      this
    }
  }
  @inline def hasSetting(setting: Setting): Boolean = args.exists(a => setting.matches(a))
  // TODO the following methods are not needed
  def hashMapLowering: Boolean = hasSetting(HashMapToSetSetting)
  def setToArray: Boolean = hasSetting(SetToArraySetting)
  def setToLinkedList: Boolean = hasSetting(SetToLinkedListSetting)
  def containerFlattenning: Boolean = hasSetting(ContainerFlattenningSetting)
  def hashMapToArray: Boolean = hasSetting(HashMapToArraySetting)
  def columnStore: Boolean = hasSetting(ColumnStoreSetting)
  def partitioning: Boolean = hasSetting(ArrayPartitioningSetting)
  def hashMapPartitioning: Boolean = hasSetting(HashMapPartitioningSetting)
  def mallocHoisting: Boolean = hasSetting(MallocHoistSetting)
  def constArray: Boolean = hasSetting(ConstantSizeArraySetting)
  def stringCompression: Boolean = hasSetting(StringDictionarySetting)
  def noLetBinding: Boolean = hasSetting(NoLetBindingSetting)
  def ifAggressive: Boolean = hasSetting(IfAggressiveSetting)
  def oldCArrayHandling: Boolean = hasSetting(CArrayAsStructSetting)
  def pointerStore: Boolean = hasSetting(PointerStoreSetting)
  def stringOptimization: Boolean = hasSetting(StringOptimizationSetting)
  def hashMapNoCollision: Boolean = hasSetting(HashMapNoCollisionSetting)
  def largeOutputHoisting: Boolean = hasSetting(LargeOutputHoistingSetting)
  def noFieldRemoval: Boolean = hasSetting(NoFieldRemovalSetting)
  def forceFieldRemoval: Boolean = hasSetting(ForceFieldRemovalSetting)
  def noSingletonHashMap: Boolean = hasSetting(NoSingletonHashMapSetting)
  def nameIsWithFlag: Boolean = hasSetting(OutputNameWithFlagSetting)
  def onlyLoading: Boolean = hasSetting(OnlyLoaderSetting)
  def chooseOptimal: Boolean = hasSetting(OptimalSetting)
  def chooseCompliant: Boolean = hasSetting(CompliantSetting)
  def targetLanguage: Language = if (hasSetting(ScalaCGSetting))
    ScalaCoreLanguage
  else
    CCoreLanguage
  def queryMonadLowering: Boolean = hasSetting(QueryMonadLoweringSetting)
  def queryMonadCPS: Boolean = hasSetting(QueryMonadCPSSetting)
  def queryMonadIterator: Boolean = hasSetting(QueryMonadIteratorSetting)
  def queryMonadOptimization: Boolean = hasSetting(QueryMonadOptSetting)
  def queryMonadHoisting: Boolean = hasSetting(QueryMonadHoistingSetting)

  def hasOptimizationLevel: Boolean = hasSetting(OptimizationLevelSetting)
  def getOptimizationLevel: Int = args.find(a => OptimizationLevelSetting.matches(a)).get.substring("-levels=".size).toInt

  def queryName: String = args(2)
}

/**
 * Contains list of available settings
 */
object Settings {
  val ALL_SETTINGS = List(HashMapToSetSetting,
    SetToArraySetting,
    SetToLinkedListSetting,
    ContainerFlattenningSetting,
    HashMapToArraySetting,
    ColumnStoreSetting,
    PointerStoreSetting,
    ArrayPartitioningSetting,
    HashMapPartitioningSetting,
    MallocHoistSetting,
    ConstantSizeArraySetting,
    StringDictionarySetting,
    NoLetBindingSetting,
    IfAggressiveSetting,
    CArrayAsStructSetting,
    StringOptimizationSetting,
    HashMapNoCollisionSetting,
    LargeOutputHoistingSetting,
    NoFieldRemovalSetting,
    NoSingletonHashMapSetting,
    OutputNameWithFlagSetting,
    OnlyLoaderSetting,
    OptimalSetting,
    CompliantSetting,
    ScalaCGSetting,
    QueryMonadLoweringSetting,
    QueryMonadIteratorSetting,
    QueryMonadCPSSetting,
    QueryMonadOptSetting,
    ForceFieldRemovalSetting,
    QueryMonadHoistingSetting,
    OptimizationLevelSetting)
}

/**
 * The main trait for every setting
 *
 * @field prefix the prefix used for a setting used in command line
 * @field flagName the name of a setting flag
 * @field description the description for a setting
 */
sealed trait Setting {
  val prefix: String
  val flagName: String
  val description: String
  def fullFlagName: String = prefix + flagName
  // TODO adaopt all places to use this method
  def matches(arg: String): Boolean = fullFlagName == arg
}

/**
 * The super class of optimization settings
 * @param flagName the name of the optimization setting
 * @param mainDescription the main part of the description for the optimization setting
 * @param extraDescription the additional description about the optimization setting
 */
abstract class OptimizationSetting(val flagName: String,
                                   val mainDescription: String,
                                   val language: Language,
                                   val extraDescription: String = "") extends Setting {
  val prefix: String = "+"
  val description: String = mainDescription + {
    if (extraDescription.length == 0)
      ""
    else
      s" ($extraDescription)"
  }
}

/**
 * The super class of option settings, which are not related to optimization
 * @param flagName the name of the option setting
 * @param desciprtion the description for the option setting
 */
abstract class OptionSetting(val flagName: String, val description: String) extends Setting {
  val prefix: String = "-"
}

/*
 * Available optimization settings
 */
case object HashMapToSetSetting extends OptimizationSetting("hm2set",
  "Lowering HashMap and MultiMap to Array of Set",
  MCHLanguage)
case object SetToArraySetting extends OptimizationSetting("set2arr",
  "Lowering Set to Array",
  MCLanguage)
case object SetToLinkedListSetting extends OptimizationSetting("set2ll",
  "Lowering Set to LinkedList",
  MCLanguage)
case object HashMapToArraySetting extends OptimizationSetting("hm2arr",
  "Lowering HashMap to 1D Array without key inside its value",
  MCHLanguage)
case object ContainerFlattenningSetting extends OptimizationSetting("cont-flat",
  "Flattening the next field of a container of a record to the record itself",
  MCLanguage)
case object ColumnStoreSetting extends OptimizationSetting("cstore",
  "Column-Store optimization",
  ScalaCoreLanguage,
  "Not finished yet!")
case object PointerStoreSetting extends OptimizationSetting("bad-rec",
  "Pointer-Store (de)optimization",
  ScalaCoreLanguage,
  "Deoptimization!")
case object ArrayPartitioningSetting extends OptimizationSetting("part",
  "Partitions an array whenever possible",
  MCHLanguage)
case object HashMapPartitioningSetting extends OptimizationSetting("hm-part",
  "Converts MultiMaps into partitioned arrays",
  MCHLanguage)
case object MallocHoistSetting extends OptimizationSetting("malloc-hoist",
  "Hoists malloc statements outside of the critical path",
  ScalaCoreLanguage)
case object ConstantSizeArraySetting extends OptimizationSetting("const-arr",
  "Transforms arrays with a small constant size into local variables",
  ScalaCoreLanguage)
case object StringDictionarySetting extends OptimizationSetting("comprStrings",
  "Creates a dictionary for strings in the loading time and transforms string operations to integer operations",
  MCLanguage)
case object NoLetBindingSetting extends OptimizationSetting("no-let",
  "Removes unnecessary let-bindings from the generated code",
  ScalaCoreLanguage)
case object IfAggressiveSetting extends OptimizationSetting("if-agg",
  "Rewrites the conditions of if statements into bitwise form instead of the original short-circuiting form",
  ScalaCoreLanguage,
  "May produce incorrect results in some queries")
case object CArrayAsStructSetting extends OptimizationSetting("old-carr",
  "Handling C arrays as a struct of pointer and length",
  ScalaCoreLanguage)
case object StringOptimizationSetting extends OptimizationSetting("str-opt",
  "Some optimizations on string operations",
  ScalaCoreLanguage,
  "Helpful for TPCH Q22")
case object HashMapNoCollisionSetting extends OptimizationSetting("hm-no-col",
  "Transforming HashMap without collisions to Array",
  MCHLanguage)
case object LargeOutputHoistingSetting extends OptimizationSetting("ignore-printing-output",
  "If the output is so large, this flag ignores the time for printing",
  CCoreLanguage)
case object NoFieldRemovalSetting extends OptimizationSetting("no-field-rem",
  "Disables the unnecessary field removal optimization",
  ScalaCoreLanguage,
  "Deoptimization!")
case object NoSingletonHashMapSetting extends OptimizationSetting("no-sing-hm",
  "Disables the singleton hashmap optimization",
  MCHLanguage,
  "Deoptimization!")
case object QueryMonadLoweringSetting extends OptimizationSetting("monad-lowering",
  "Enables Query Monad Lowering",
  QMonadLanguage)
case object QueryMonadCPSSetting extends OptimizationSetting("monad-cps",
  "Enables Query Monad CPS Lowering",
  QMonadLanguage)
case object QueryMonadIteratorSetting extends OptimizationSetting("monad-iterator",
  "Enables Query Monad Iterator Lowering",
  QMonadLanguage)
case object QueryMonadOptSetting extends OptimizationSetting("monad-opt",
  "Enables Query Monad Optimizations",
  QMonadLanguage)
case object ForceFieldRemovalSetting extends OptimizationSetting("force-field-removal",
  "Enables Field Removal",
  QMonadLanguage)
case object QueryMonadHoistingSetting extends OptimizationSetting("monad-hoist",
  "Enables Query Monad Hoisting",
  // QMonadLanguage)
  MCHLanguage)

/* 
 * Available option settings
 */
case object OutputNameWithFlagSetting extends OptionSetting("name-with-flag",
  "Appends the optimization flags to the name of files")
case object OnlyLoaderSetting extends OptionSetting("only-load",
  "Generates only the loader of a query")
case object OptimalSetting extends OptionSetting("optimal",
  "Considers the best combiniation of optimization flags")
case object CompliantSetting extends OptionSetting("compliant",
  "Considers the best compliant combiniation of optimization flags")
case object ScalaCGSetting extends OptionSetting("scala",
  "Generates Scala code instead of C code")
case object OptimizationLevelSetting extends OptionSetting("levels",
  "The level of optimization") {
  override def matches(arg: String): Boolean = arg.startsWith("-levels=")
}

