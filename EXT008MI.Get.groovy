
/**
 * README
 *
 * Name: EXT008MI.Get
 * Description: get a record in EXT008
 * Date                         Changed By                    Description
 * 20250604                     ddecosterd@hetic3.fr     		création
 */
public class GET extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;

	public GET(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility) {
		this.mi = mi;
		this.program = program;
		this.database = database;
		this.utility = utility;
	}

	public void main() {
		Integer cono = mi.in.get("CONO");
		String  faci = (mi.inData.get("FACI") == null) ? "" : mi.inData.get("FACI").trim();
		Long mere = mi.in.get("MERE");
		String  plgr = (mi.inData.get("PLGR") == null) ? "" : mi.inData.get("PLGR").trim();

		if(cono == null) {
			mi.error("La division est obligatoire.");
			return;
		}

		if(faci.isBlank()) {
			mi.error("L'établissement est obligatoire.");
			return;
		}

		if(mere == null) {
			mi.error("La note mère est obligatoire.");
			return;
		}

		if(plgr == null) {
			mi.error("Le code PLGR est obligatoire.");
			return;
		}

		DBAction ext008Record = database.table("EXT008").index("00").selection("EXSTYL","EXITDS","EXTYPE","EXNBOF","EXPRIO","EXSORT","EXBIPE").build();
		DBContainer ext008Container = ext008Record.createContainer();
		ext008Container.setInt("EXCONO", cono);
		ext008Container.setString("EXFACI", faci);
		ext008Container.setLong("EXMERE", mere);
		ext008Container.setString("EXPLGR", plgr);

		if(ext008Record.read(ext008Container)){
			mi.getOutData().put("CONO", cono.toString());
			mi.getOutData().put("FACI",faci);
			mi.getOutData().put("MERE",mere.toString());
			mi.getOutData().put("PLGR", plgr);
			mi.getOutData().put("STYL", ext008Container.getString("EXSTYL"));
			mi.getOutData().put("ITDS",ext008Container.getString("EXITDS"));
			mi.getOutData().put("TYPE", ext008Container.getString("EXTYPE"));
			mi.getOutData().put("NBOF", ext008Container.get("EXNBOF").toString());
			mi.getOutData().put("PRIO", ext008Container.get("EXPRIO").toString());
			mi.getOutData().put("SORT", ext008Container.get("EXSORT").toString());
			mi.getOutData().put("BIPE", ext008Container.get("EXBIPE").toString());
			mi.write();
		}
		else{
			mi.error("Enregistrement inexistant !");
			return;
		}
	}

}