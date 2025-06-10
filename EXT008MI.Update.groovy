
/**
 * README
 *
 * Name: EXT008MI.Update
 * Description: Update a record in EXT008
 * Date                         Changed By                    Description
 * 20250604                     ddecosterd@hetic3.fr     		création
 */
public class UPDATE extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;

	public UPDATE(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility) {
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

		if(cono == null) {
			mi.error("La division est obligatoire.");
			return;
		}

		if(faci.isBlank()) {
			mi.error("L'établissement est obligatoire.");
			return;
		}

		if(plgr.isBlank()) {
			mi.error("Le code PLGR est obligatoire.");
			return;
		}

		if(mere == null) {
			mi.error("La note mère est obligatoire");
			return;
		}

		DBAction ext008Record = database.table("EXT008").index("00").build();
		DBContainer ext008Container = ext008Record.createContainer();
		ext008Container.setInt("EXCONO", cono);
		ext008Container.setString("EXFACI", faci);
		ext008Container.setLong("EXMERE", mere);

		boolean updatable = ext008Record.readLock(ext008Container, { LockedResult updateRecoord ->
			updateRecoord.setString("EXPLGR", plgr);
			updateRecoord.setString("EXSTYL", styl);
			updateRecoord.setString("EXITDS", itds);
			updateRecoord.setString("EXTYPE", type);
			if(nbof != null)
				updateRecoord.setInt("EXNBOF", nbof);
			if(prio != null)
				updateRecoord.setInt("EXPRIO", prio);
			if(sort != null)
				updateRecoord.setInt("EXSORT", sort);
			int CHNO = updateRecoord.getInt("EXCHNO");
			if(CHNO== 999) {CHNO = 0;}
			CHNO++;
			updateRecoord.set("EXLMDT", (Integer) utility.call("DateUtil", "currentDateY8AsInt"));
			updateRecoord.set("EXCHID", program.getUser());
			updateRecoord.setInt("EXCHNO", CHNO);
			updateRecoord.update();
		});

		if(!updatable)
		{
			mi.error("L'enregistrement n'existe pas.");
			return;
		}

	}

}