
/**
 * README
 *
 * Name: EXT008MI.List
 * Description: list records in EXT008
 * Date                         Changed By                    Description
 * 20250604                     ddecosterd@hetic3.fr     		création
 */
public class LIST extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;

	public LIST(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility) {
		this.mi = mi;
		this.program = program;
		this.database = database;
		this.utility = utility;
	}

	public void main() {
		Integer cono = mi.in.get("CONO");
		String  faci = (mi.inData.get("FACI") == null) ? "" : mi.inData.get("FACI").trim();
		String  plgr = (mi.inData.get("PLGR") == null) ? "" : mi.inData.get("PLGR").trim();

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

		DBAction ext008Record = database.table("EXT008").index("10").selection("EXSTYL","EXITDS","EXTYPE","EXNBOF","EXPRIO","EXSORT").build();
		DBContainer ext008Container = ext008Record.createContainer();
		ext008Container.setInt("EXCONO", cono);
		ext008Container.setString("EXFACI", faci);
		ext008Container.setString("EXPLGR", plgr);

		int nrOfRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000? 10000: mi.getMaxRecords();

		ext008Record.readAll(ext008Container, 3, nrOfRecords,{ DBContainer container ->
			mi.getOutData().put("CONO", cono.toString());
			mi.getOutData().put("FACI",faci);
			mi.getOutData().put("PLGR", plgr);
			mi.getOutData().put("MERE",container.get("EXMERE").toString());
			mi.getOutData().put("STYL", container.getString("EXSTYL"));
			mi.getOutData().put("ITDS",container.getString("EXITDS"));
			mi.getOutData().put("TYPE", container.getString("EXTYPE"));
			mi.getOutData().put("NBOF", container.get("EXNBOF").toString());
			mi.getOutData().put("PRIO", container.get("EXPRIO").toString());
			mi.getOutData().put("SORT", container.get("EXSORT").toString());
			mi.write();
		});
	}

}