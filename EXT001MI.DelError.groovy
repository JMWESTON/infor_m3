/**
 * README
 *
 * Name: EXT001MI.DelError
 * Description:
 * Date                         Changed By                         Description
 * 20231001                     j.quersin@3kles-consulting.com     delete a record in EXT001 table
 */
public class DelError extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;
	private final MICallerAPI miCaller;

	public DelError(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility, MICallerAPI miCaller) {
		this.mi = mi;
		this.program = program;
		this.database = database;
		this.utility = utility;
		this.miCaller = miCaller;
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

		// check authorized values for MODE
		if(MODE!=0 && MODE!=1) {
			this.mi.error("Mode is incorrect (0 or 1).");
			return;
		}
		// when MODE = 0, error number is mandatory
		if(MODE== 0 && ERNU==null){
			this.mi.error("Error number is mandatory for mode 0.");
			return;
		}
		// mode 1 parameters validation
		if(MODE==1){
			// HAND parameter mandatory for mode 1, value must be 0 or 1
			if(HAND!=0 && HAND!=1 && HAND!=2 ) {
				this.mi.error("HAND parameter is incorrect (0,1 or 2).");
				return;
			}
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
			Boolean recordExists = false;
			recordExists = xtRecord.readLock(xtContainer, {LockedResult entry ->
				if(!entry.delete()) {
					this.mi.error("Record does not exist");
				}
			});
			if(!recordExists) {
				this.mi.error("Record does not exist");
			}
		}
		if(MODE==1) {
			ExpressionFactory EXT001ExpressionFactory = database.getExpressionFactory("EXT001");
			EXT001ExpressionFactory = EXT001ExpressionFactory.eq("EXHAND", HAND.toString());
			EXT001ExpressionFactory = EXT001ExpressionFactory.and(EXT001ExpressionFactory.ge("EXRGDT",FRDT.toString()));
			EXT001ExpressionFactory = EXT001ExpressionFactory.and(EXT001ExpressionFactory.le("EXRGDT",TODT.toString()));

			DBAction xtRecord = this.database.table("EXT001").index("00").matching(EXT001ExpressionFactory).selectAllFields().build();

			DBContainer xtContainer = xtRecord.createContainer();
			xtContainer.set("EXCONO", CONO);

			Closure<?> EXT001Closure= { DBContainer EXT001data ->
				DBAction deletion = this.database.table("EXT001").index("00").build();
				DBContainer EXT001record = deletion.createContainer();
				EXT001record.set("EXCONO", EXT001data.get("EXCONO"));
				EXT001record.set("EXERNU", EXT001data.get("EXERNU"));
				deletion.readLock(EXT001record,{LockedResult entry ->
					entry.delete();
				})
			}
			if(xtRecord.readAll(xtContainer,1,EXT001Closure)==0){
				this.mi.error("No record matching required criterias found.");
			}
		}
	}
}
