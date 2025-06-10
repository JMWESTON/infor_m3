
/**
 * README
 *
 * Name: EXT008MI.DelAll
 * Description: Del all records in EXT008
 * Date                         Changed By                    Description
 * 20250604                     ddecosterd@hetic3.fr     		création
 */
public class DELALL extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;

	public DELALL(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility) {
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

		int nbKeys = 2;

		DBAction ext008Record = database.table("EXT008").index("00").build();
		DBContainer ext008Container = ext008Record.createContainer();
		ext008Container.setInt("EXCONO", cono);
		ext008Container.setString("EXFACI", faci);

		if(!plgr.isBlank()) {
			ext008Container.setString("EXPLGR", plgr);
			nbKeys = 3;
		}

		int deleted = ext008Record.readAll(ext008Container,nbKeys, 10000, { DBContainer container ->
			ext008Record.readLock(container, { LockedResult lockedResult ->
				lockedResult.delete();
			});
		});

		mi.getOutData().put("NDEL", deleted.toString());
	}

}