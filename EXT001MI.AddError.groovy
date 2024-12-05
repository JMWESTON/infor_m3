/**
 * README
 *
 * Name: EXT001.AddError
 * Description:
 * Date                         Changed By                         Description
 * 20231001                     j.quersin@3kles-consulting.com     add a record in EXT001 xtend table
 */
public class AddError extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;
	private final MICallerAPI miCaller;

	public AddError(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility, MICallerAPI miCaller) {
		this.mi = mi;
		this.program = program;
		this.database = database;
		this.utility = utility;
		this.miCaller = miCaller;
	}

	public void main() {
		Integer CONO = this.mi.in.get("CONO");
		String  IFID = this.mi.inData.get("IFID");
		String  FILE = this.mi.inData.get("FILE");
		String  ERRM = this.mi.inData.get("ERRM");
		Integer HAND = 0;
		String  ERNU = "";
		Boolean CONOExists;

		// check if CONO exists
		def MNS095MIGetcallback = {Map<String, String> response ->
			(response.CONO==null) ? (CONOExists = false): (CONOExists = true);
		}
		miCaller.call("MNS095MI","Get", ["CONO":CONO.toString()], MNS095MIGetcallback)
		if (!CONOExists) {
			this.mi.error("CONO "+CONO+" does not exist.");
			return;
		}

		// get a record ID from CRS165/Z9/1, return an error when not found
		def CRS165MIRtvNextNumbercallback = {Map<String, String> response ->
			if(response.NBNR != null){
				ERNU = response.NBNR;
			}
		}
		miCaller.call("CRS165MI","RtvNextNumber", ["NBTY":"Z9","NBID":"1"], CRS165MIRtvNextNumbercallback)
		if (ERNU.equals("")){
			this.mi.error("Unable to Retrieve Z9/1 counter, please check CRS165.");
			return;
		}

		DBAction xtRecord = this.database.table("EXT001").index("00").selectAllFields().build();

		DBContainer xtContainer = xtRecord.createContainer();
		xtContainer.set("EXCONO", CONO);
		xtContainer.set("EXIFID", IFID);
		xtContainer.set("EXFILE", FILE);
		xtContainer.set("EXERRM", ERRM);
		xtContainer.set("EXERNU", ERNU.toLong());

		// Set the change tracking fields
		xtContainer.set("EXRGDT", (Integer) this.utility.call("DateUtil", "currentDateY8AsInt"));
		xtContainer.set("EXLMDT", (Integer) this.utility.call("DateUtil", "currentDateY8AsInt"));
		xtContainer.set("EXCHID", this.program.getUser());
		xtContainer.set("EXRGTM", (Integer) this.utility.call("DateUtil", "currentTimeAsInt"));
		xtContainer.set("EXCHNO", 1);

		if(!xtRecord.insert(xtContainer)) {
			this.mi.error("Record already exist");
		}
	}
}