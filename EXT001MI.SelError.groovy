/**
 * README
 *
 * Name: EXT001MI.SelError
 * Description:
 * Date                         Changed By                         Description
 * 20231006                     j.quersin@3kles-consulting.com     select a record in EXT001 table
 */
public class SelError extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;
	private final MICallerAPI miCaller;

	public SelError(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility, MICallerAPI miCaller) {
		this.mi = mi;
		this.program = program;
		this.database = database;
		this.utility = utility;
		this.miCaller = miCaller;
	}

	public void main() {
		Integer CONO = this.mi.in.get("CONO") as Integer;
		Long    ERNU = this.mi.in.get("ERNU") as Long;
		Integer MODE = this.mi.in.get("MODE") as Integer;
		Long    FRDT = this.mi.in.get("FRDT") as Long;
		Long    TODT = this.mi.in.get("TODT") as Long;
		Integer HAND = this.mi.in.get("HAND") as Integer;
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
		// when MODE = 0, error number is mandatory
		if(MODE== 0 && ERNU==null){
			this.mi.error("Error number is mandatory for mode 0.");
			return;
		}
		// HAND parameter value must be 0 or 1 or 2
		if(HAND!=0&&HAND!=1&&HAND!=2) {
			this.mi.error("HAND parameter is incorrect (0, 1 or 2).");
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
			if(!this.utility.call("DateUtil", "isDateValid",FRDT.toString(),"yyyyMMdd")){
				this.mi.error(FRDT+" is not valid");
				return;
			}
			if(!this.utility.call("DateUtil", "isDateValid",TODT.toString(),"yyyyMMdd")){
				this.mi.error(TODT+" is not valid");
				return;
			}
		}
		if(MODE==0) {
			DBAction xtRecord = this.database.table("EXT001").index("00").selectAllFields().build();

			DBContainer xtContainer = xtRecord.createContainer();
			xtContainer.set("EXCONO", CONO);
			xtContainer.set("EXERNU", ERNU);

			if(xtRecord.read(xtContainer)) {
				for(String field in ["ERRM", "FILE","IFID","HAND","ERNU"]) {
					this.mi.getOutData().put(field, xtContainer.get("EX" + field).toString());
				}
				this.mi.write();
			}
		}
		if(MODE==1) {
			ExpressionFactory EXT001ExpressionFactory = database.getExpressionFactory("EXT001");
			EXT001ExpressionFactory = EXT001ExpressionFactory.ge("EXRGDT",FRDT.toString());
			EXT001ExpressionFactory = EXT001ExpressionFactory.and(EXT001ExpressionFactory.le("EXRGDT",TODT.toString()));
			if(HAND==0 || HAND==1) {EXT001ExpressionFactory = EXT001ExpressionFactory.and(EXT001ExpressionFactory.eq("EXHAND", HAND.toString()));
			}

			DBAction xtRecord = this.database.table("EXT001").index("00").matching(EXT001ExpressionFactory).selectAllFields().build();

			DBContainer xtContainer = xtRecord.createContainer();
			xtContainer.set("EXCONO", CONO);

			int pageSize = mi.getMaxRecords() <= 0 ? 1000 : mi.getMaxRecords();
			xtRecord.readAll(xtContainer, 1, pageSize, { DBContainer entry ->
				this.fillOutLine(this.mi.getOutData(), entry);
				this.mi.write();
			});
		}
	}
	// return values from EXT001
	private void fillOutLine(Map<?, ?> outData, final DBContainer dbLine) {
		for(String field in ["ERRM", "FILE","IFID","HAND","ERNU","RGDT","RGTM", "CHNO","LMDT"]) {
			this.mi.getOutData().put(field, dbLine.get("EX" + field).toString());
		}
	}
}
