/**
 * README
 *
 * Name: EXT001MI.UpdError
 * Description:
 * Date                         Changed By                         Description
 * 20231006                     j.quersin@3kles-consulting.com     update a record in EXT001 table
 */
public class UpdError extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;
	private final MICallerAPI miCaller;
	private final LoggerAPI logger;

	public UpdError(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility, MICallerAPI miCaller, LoggerAPI logger) {
		this.mi = mi;
		this.program = program;
		this.database = database;
		this.utility = utility;
		this.miCaller = miCaller;
		this.logger = logger;
	}

	public void main() {
		Integer CONO = this.mi.in.get("CONO");
		Long    ERNU = this.mi.in.get("ERNU");
		Integer MODE = this.mi.in.get("MODE");
		Long    FRDT = this.mi.in.get("FRDT");
		Long    TODT = this.mi.in.get("TODT");
		Integer HAND = this.mi.in.get("HAND");
		Boolean CONOExists;

		// check if CONO exists
		def MNS095MIGetcallback = {Map<String, String> response ->
			(response.CONO==null) ? (CONOExists = false): (CONOExists = true);
		}
		miCaller.call("MNS095MI","Get", ["CONO":CONO.toString()], MNS095MIGetcallback);
		if (!CONOExists) {
			this.mi.error("CONO "+CONO+" does not exist.");
			return;
		}
		if(MODE!=0 && MODE!=1) {
			this.mi.error("Mode is incorrect (0 or 1).");
			return;
		}
		// HAND parameter value must be 0 or 1
		if(HAND!=0 && HAND!=1) {
			this.mi.error("HAND parameter is incorrect (0 or 1).");
			return;
		}
		// when MODE = 0, error number is mandatory
		if(MODE== 0 && ERNU==null){
			this.mi.error("Error number is mandatory for mode 0.");
			return;
		}
		if(MODE==1){

			// date range mandatory for mode 1
			if(FRDT == null ){
				this.mi.error("FRDT is mandatory for mode 1");
				return;
			}
			if(TODT == null ){
				this.mi.error("TODT is mandatory for mode 1");
				return;
			}
			if(!this.utility.call("DateUtil", "isDateValid",FRDT,"yyyyMMdd")){
				this.mi.error(FRDT+" is not valid");
				return;
			}
			if(!this.utility.call("DateUtil", "isDateValid",TODT,"yyyyMMdd")){
				this.mi.error(TODT+" is not valid");
				return;
			}
		}
		if(MODE==0) {
			DBAction xtRecord = this.database.table("EXT001").index("00").selectAllFields().build();

			DBContainer xtContainer = xtRecord.createContainer();
			xtContainer.set("EXCONO", CONO);
			xtContainer.set("EXERNU", ERNU);
			Boolean recordExists = false

			recordExists = xtRecord.readLock(xtContainer, {LockedResult updatedRecord ->
				String CHNO = updatedRecord.get("EXCHNO").toString();
				if(CHNO.equals("999")) {CHNO = "0";}

				updatedRecord.set("EXLMDT", (Integer) this.utility.call("DateUtil", "currentDateY8AsInt"));
				updatedRecord.set("EXCHID", this.program.getUser());
				updatedRecord.set("EXCHNO", Integer.parseInt(CHNO)+1);
				updatedRecord.set("EXHAND", HAND);
				updatedRecord.update();
			});
			if(!recordExists) {
				this.mi.error("Record does not exist");
			}
		}
		if(MODE==1) {
			ExpressionFactory EXT001ExpressionFactory = database.getExpressionFactory("EXT001");
			EXT001ExpressionFactory = EXT001ExpressionFactory.ge("EXRGDT",FRDT.toString());
			EXT001ExpressionFactory = EXT001ExpressionFactory.and(EXT001ExpressionFactory.le("EXRGDT",TODT.toString()));

			DBAction xtRecord = this.database.table("EXT001").index("00").matching(EXT001ExpressionFactory).selectAllFields().build();

			DBContainer xtContainer = xtRecord.createContainer();
			xtContainer.set("EXCONO", CONO);

			Closure<?> EXT001Closure= { DBContainer EXT001data ->
				DBAction update = this.database.table("EXT001").index("00").build();
				DBContainer EXT001record = update.createContainer();
				EXT001record.set("EXCONO", EXT001data.get("EXCONO"));
				EXT001record.set("EXERNU", EXT001data.get("EXERNU"));

				update.readLock(EXT001record,{LockedResult updatedRecord ->
					String CHNO = updatedRecord.get("EXCHNO").toString();
					if(CHNO.equals("999")) {CHNO = "0";}
					updatedRecord.set("EXLMDT", (Integer) this.utility.call("DateUtil", "currentDateY8AsInt"));
					updatedRecord.set("EXCHID", this.program.getUser());
					updatedRecord.set("EXCHNO", Integer.parseInt(CHNO)+1);
					updatedRecord.set("EXHAND", HAND);
					updatedRecord.update();
				})
			}
			if(xtRecord.readAll(xtContainer,1,EXT001Closure)==0){
				this.mi.error("No record matching required criterias found.");
			}
		}
	}
}
