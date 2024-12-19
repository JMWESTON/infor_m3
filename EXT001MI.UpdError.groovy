/**
* README
*
* Name: EXT001MI.UpdError
* Description: 
* Date                         Changed By                         Description
* 20231006                     j.quersin@3kles-consulting.com     update a record in EXT001 table
* 20241210                     j.quersin@3kles-consulting.com     code review
*/
public class UpdError extends ExtendM3Transaction {
  private final MIAPI mi;
  private final ProgramAPI program;
  private final DatabaseAPI database;
  private final UtilityAPI utility;
  private final MICallerAPI miCaller
  private final LoggerAPI logger
  
  public UpdError(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility, MICallerAPI miCaller, LoggerAPI logger) {
    this.mi = mi;
    this.program = program;
    this.database = database;
    this.utility = utility;
    this.miCaller = miCaller
    this.logger = logger;
  }
  
  public void main() {
    // Retrieve input fields
    Integer CONO = mi.in.get("CONO");
    Long    ERNU = mi.in.get("ERNU");
    Integer MODE = mi.in.get("MODE");
    Long    FRDT = mi.in.get("FRDT");
    Long    TODT = mi.in.get("TODT");
    Integer HAND = mi.in.get("HAND");
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
    // HAND parameter value must be 0 or 1
    if(HAND!=0 && HAND!=1) {
      mi.error("HAND parameter is incorrect (0 or 1).")
      return;
    }
    // when MODE = 0, error number is mandatory
    if(MODE== 0 && ERNU==null){
      mi.error("Error number is mandatory for mode 0.")
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
      if(!utility.call("DateUtil", "isDateValid",FRDT,"yyyyMMdd")){
        mi.error(FRDT+" is not valid");
        return;
      }
      // Check date validity for TODT
      if(!utility.call("DateUtil", "isDateValid",TODT,"yyyyMMdd")){
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
      Boolean recordExists = false
      // Perform the update
      recordExists = xtRecord.readLock(xtContainer, {LockedResult updatedRecord -> 
        String CHNO = updatedRecord.get("EXCHNO").toString();
        if(CHNO.equals("999")) {CHNO = "0";}
        
        // Set the change tracking fields
        updatedRecord.set("EXLMDT", (Integer) utility.call("DateUtil", "currentDateY8AsInt"));
        updatedRecord.set("EXCHID", program.getUser());
        updatedRecord.set("EXCHNO", Integer.parseInt(CHNO)+1);
			  updatedRecord.set("EXHAND", HAND);
  			updatedRecord.update();
      });
      if(!recordExists) {
        mi.error("Record does not exist");
        return;
      }
    }
    if(MODE==1) {
      // Expression factory 
      ExpressionFactory EXT001ExpressionFactory = database.getExpressionFactory("EXT001");
      EXT001ExpressionFactory = EXT001ExpressionFactory.ge("EXRGDT",FRDT.toString());
      EXT001ExpressionFactory = EXT001ExpressionFactory.and(EXT001ExpressionFactory.le("EXRGDT",TODT.toString()));
      
      // Create handles to database
      DBAction xtRecord = database.table("EXT001").index("00").matching(EXT001ExpressionFactory).selectAllFields().build();
      
      // Create and initialize the containers : xtend
      DBContainer xtContainer = xtRecord.createContainer();
      xtContainer.set("EXCONO", CONO);
      
      // Closure for update
      Closure<?> EXT001Closure= { DBContainer EXT001data ->
			  DBAction update = database.table("EXT001").index("00").build();
			  DBContainer EXT001record = update.createContainer();
				EXT001record.set("EXCONO", EXT001data.get("EXCONO"));
				EXT001record.set("EXERNU", EXT001data.get("EXERNU"));
				
				// Perform the update
				update.readLock(EXT001record,{LockedResult updatedRecord -> 
				  String CHNO = updatedRecord.get("EXCHNO").toString();
          if(CHNO.equals("999")) {CHNO = "0";}
          // Set the change tracking fields
          updatedRecord.set("EXLMDT", (Integer) utility.call("DateUtil", "currentDateY8AsInt"));
          updatedRecord.set("EXCHID", program.getUser());
          updatedRecord.set("EXCHNO", Integer.parseInt(CHNO)+1);
				  updatedRecord.set("EXHAND", HAND);
				  updatedRecord.update();
				})
			}
			int pageSize = mi.getMaxRecords() <= 0 ? 1000 : mi.getMaxRecords()
      if(xtRecord.readAll(xtContainer,1,pageSize,EXT001Closure)==0){
        mi.error("No record matching required criterias found.");
        return;
      }
    }
  }
}