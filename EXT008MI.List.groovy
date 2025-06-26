
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
		Long mere = mi.in.get("MERE");
		String  plgr = (mi.inData.get("PLGR") == null) ? "" : mi.inData.get("PLGR").trim();
		Integer bipe = mi.in.get("BIPE");

		if(cono == null) {
			mi.error("La division est obligatoire.");
			return;
		}

		if(faci.isBlank()) {
			mi.error("L'établissement est obligatoire.");
			return;
		}

		if(!plgr.isBlank() && mere != null) {
			mi.error("Les champs PLGR et MERE ne peuvent être renseignés en même temps.");
			return;
		}

		int nbKeys = 2;
		if(!plgr.isBlank() || !mere != null) {
			nbKeys++;
		}

		String index = "00";
		if(!plgr.isBlank()) {
			index = "10";
		}

		ExpressionFactory ext008ExpressionFactory = database.getExpressionFactory("EXT008");
		if(bipe != null && bipe >= 0 && bipe <= 1)
			ext008ExpressionFactory = ext008ExpressionFactory.eq("EXBIPE", bipe.toString());
		DBAction ext008Record = database.table("EXT008").index(index).matching(ext008ExpressionFactory).selection("EXSTYL","EXITDS","EXTYPE","EXNBOF","EXPRIO","EXSORT","EXBIPE").build();
		DBContainer ext008Container = ext008Record.createContainer();
		ext008Container.setInt("EXCONO", cono);
		ext008Container.setString("EXFACI", faci);
		if(!plgr.isBlank())
			ext008Container.setString("EXPLGR", plgr);
		if(mere != null)
			ext008Container.setLong("EXMERE", mere);

		int nrOfRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000? 10000: mi.getMaxRecords();

		ext008Record.readAll(ext008Container, nbKeys, nrOfRecords,{ DBContainer container ->
			mi.getOutData().put("CONO", cono.toString());
			mi.getOutData().put("FACI",faci);
			mi.getOutData().put("PLGR", container.getString("EXPLGR"));
			mi.getOutData().put("MERE",container.get("EXMERE").toString());
			mi.getOutData().put("STYL", container.getString("EXSTYL"));
			mi.getOutData().put("ITDS",container.getString("EXITDS"));
			mi.getOutData().put("TYPE", container.getString("EXTYPE"));
			mi.getOutData().put("NBOF", container.get("EXNBOF").toString());
			mi.getOutData().put("PRIO", container.get("EXPRIO").toString());
			mi.getOutData().put("SORT", container.get("EXSORT").toString());
			mi.getOutData().put("BIPE", container.get("EXBIPE").toString());
			mi.write();
		});
	}

}