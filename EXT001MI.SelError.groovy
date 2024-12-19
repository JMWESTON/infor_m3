/**
* README
*
* Name: EXT001MI.SelError
* Description: 
* Date                         Changed By                         Description
* 20231006                     j.quersin@3kles-consulting.com     select a record in EXT001 table
* 20241210                     j.quersin@3kles-consulting.com     code review
*/
public class SelError extends ExtendM3Transaction {
  private final MIAPI mi;
  private final ProgramAPI program;
  private final DatabaseAPI database;
  private final UtilityAPI utility;
  private final MICallerAPI miCaller
  private final LoggerAPI logger
  
  public SelError(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility, MICallerAPI miCaller, LoggerAPI logger) {
    this.mi = mi;
    this.program = program;
    this.database = database;
    this.utility = utility;
    this.miCaller = miCaller
    this.logger = logger;
  }
  
  public void main() {
    // Retrieve input fields
    Integer CONO = mi.in.get("CONO") as Integer;
    Long    ERNU = mi.in.get("ERNU") as Long;
    Integer MODE = mi.in.get("MODE") as Integer;
    Long    FRDT = mi.in.get("FRDT") as Long;
    Long    TODT = mi.in.get("TODT") as Long;
    Integer HAND = mi.in.get("HAND") as Integer;
    Boolean CONOExists;
    
    // check if CONO exists
    Closure<?> MNS095MIGetcallback = {
      Map<String, String> response ->
      (response.CONO==null) ? (CONOExists = false): (CONOExists = true);
    }
    miCaller.call("MNS095MI","Get", ["CONO":CONO.toString()], MNS095MIGetcallback);   
    if (!CONOExists) {
      mi.error("CONO "+CONO+" does not exist.");
      return;
    }
    // check authorized values for MODE
    if(MODE!=0 && MODE!=1) {
      mi.error("Mode is incorrect (0 or 1).")
      return;
    }
    // when MODE = 0, error number is mandatory
    if(MODE== 0 && ERNU==null){
      mi.error("Error number is mandatory for mode 0.")
      return;
    }
    // HAND parameter value must be 0 or 1 or 2
    if(HAND!=0&&HAND!=1&&HAND!=2) {
      mi.error("HAND parameter is incorrect (0, 1 or 2).")
      return;
    }
    // mode 1 parameters validation
    if(MODE==1){
      // date range mandatory for mode 1
      if(FRDT == null ){
        mi.error("FRDT is mandatory for mode 1");
        return;
      }
      if(TODT == null ){
        mi.error("TODT is mandatory for mode 1");
        return;
      }
      // Check date validity for FRDT
      if(!utility.call("DateUtil", "isDateValid",FRDT.toString(),"yyyyMMdd")){
        mi.error(FRDT+" is not valid");
        return;
      }
      // Check date validity for TODT
      if(!utility.call("DateUtil", "isDateValid",TODT.toString(),"yyyyMMdd")){
        mi.error(TODT+" is not valid");
        return;
      }
    }
    if(MODE==0) {
      // Create handles to database
      DBAction xtRecord = database.table("EXT001").index("00").selectAllFields().build();
      
      // Create and initialize the containers : xtend
      DBContainer xtContainer = xtRecord.createContainer();
      xtContainer.set("EXCONO", CONO);
      xtContainer.set("EXERNU", ERNU);  
      // Perform the read
      if(xtRecord.read(xtContainer)) {
        for(String field in ["ERRM", "FILE","IFID","HAND","ERNU"]) {
          mi.getOutData().put(field, xtContainer.get("EX" + field).toString());
        }
        mi.write();
      }
    }
    if(MODE==1) {
      // Expression factory 
      ExpressionFactory EXT001ExpressionFactory = database.getExpressionFactory("EXT001")
      EXT001ExpressionFactory = EXT001ExpressionFactory.ge("EXRGDT",FRDT.toString())
      EXT001ExpressionFactory = EXT001ExpressionFactory.and(EXT001ExpressionFactory.le("EXRGDT",TODT.toString()))
      if(HAND==0 || HAND==1) {EXT001ExpressionFactory = EXT001ExpressionFactory.and(EXT001ExpressionFactory.eq("EXHAND", HAND.toString()));}
      
      // Create handles to database
      DBAction xtRecord = database.table("EXT001").index("00").matching(EXT001ExpressionFactory).selectAllFields().build();
      
      // Create and initialize the containers : xtend
      DBContainer xtContainer = xtRecord.createContainer();
      xtContainer.set("EXCONO", CONO);
      
      int pageSize = mi.getMaxRecords() <= 0 ? 1000 : mi.getMaxRecords()
      xtRecord.readAll(xtContainer, 1, pageSize, { DBContainer entry ->
        fillOutLine(mi.getOutData(), entry);
        mi.write();
      });
    }
  }
  // return values from EXT001
  private void fillOutLine(Map<?, ?> outData, final DBContainer dbLine) {
    for(String field in ["ERRM", "FILE","IFID","HAND","ERNU","RGDT","RGTM", "CHNO","LMDT"]) {
        mi.getOutData().put(field, dbLine.get("EX" + field).toString());
    }
  }
}