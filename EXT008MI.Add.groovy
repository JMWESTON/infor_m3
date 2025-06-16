
/**
 * README
 *
 * Name: EXT008MI.Add
 * Description: Add a record in EXT008
 * Date                         Changed By                    Description
 * 20250604                     ddecosterd@hetic3.fr     		création
 */
public class ADD extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;

	public ADD(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility) {
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
		String  styl = (mi.inData.get("STYL") == null) ? "" : mi.inData.get("STYL").trim();
		String  itds = (mi.inData.get("ITDS") == null) ? "" : mi.inData.get("ITDS").trim();
		String  type = (mi.inData.get("TYPE") == null) ? "" : mi.inData.get("TYPE").trim();
		Integer nbof = mi.in.get("NBOF");
		Integer prio = mi.in.get("PRIO");
		Integer sort = mi.in.get("SORT");
		Integer bipe = mi.in.get("BIPE");

		if(cono == null) {
			mi.error("La division est obligatoire.");
			return;
		}
		if(!utility.call("CheckUtil", "checkConoExist", database, cono)) {
			mi.error("La division est inexistante.");
			return;
		}

		if(faci.isBlank()) {
			mi.error("L'établissement est obligatoire.");
			return;
		}
		if(!utility.call("CheckUtil", "checkFacilityExist", database, cono, faci)) {
			mi.error("L'établissement est inexistant.");
			return;
		}


		if(plgr.isBlank()) {
			mi.error("Le code PLGR est obligatoire.");
			return;
		}
		if(!utility.call("CheckUtil", "checkPLGRExist", database, cono, faci, plgr)) {
			mi.error("Le code PLGR est inexistant.");
			return;
		}

		if(mere == null) {
			mi.error("La note mère est obligatoire");
			return;
		}
		if(!utility.call("CheckUtil", "checkSCHNExist", database, cono, mere)){
			mi.error("La note mère n'existe pas.");
			return;
		}

		if( prio != null && (prio < 0 || prio > 9)) {
			mi.error("La valeur du champ PRIO doit être compris entre 0 et 9.");
			return;
		}

		if( bipe != null && (bipe < 0 || bipe > 1)) {
			mi.error("La valeur du champ BIPE doit être compris entre 0 et 1.");
			return;
		}

		DBAction ext008Record = database.table("EXT008").index("00").build();
		DBContainer ext008Container = ext008Record.createContainer();
		ext008Container.setInt("EXCONO", cono);
		ext008Container.setString("EXFACI", faci);
		ext008Container.setString("EXPLGR", plgr);
		ext008Container.setLong("EXMERE", mere);

		if(!ext008Record.read(ext008Container)){
			ext008Container.setString("EXSTYL", styl);
			ext008Container.setString("EXITDS", itds);
			ext008Container.setString("EXTYPE", type);
			if(nbof != null)
				ext008Container.setInt("EXNBOF", nbof);
			if(prio != null)
				ext008Container.setInt("EXPRIO", prio);
			if(sort != null)
				ext008Container.setInt("EXSORT", sort);
			if(bipe != null)
				ext008Container.setInt("EXBIPE", bipe);
			ext008Container.set("EXRGDT", (Integer) utility.call("DateUtil", "currentDateY8AsInt"));
			ext008Container.set("EXLMDT", (Integer) utility.call("DateUtil", "currentDateY8AsInt"));
			ext008Container.set("EXCHID", program.getUser());
			ext008Container.set("EXRGTM", (Integer) utility.call("DateUtil", "currentTimeAsInt"));
			ext008Container.set("EXCHNO", 1);
			ext008Record.insert(ext008Container);
		}else
		{
			mi.error("Enregistrement existe déjà.");
			return;
		}

	}

}