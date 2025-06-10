/**
 * README
 *
 * Name: EXT005MI.DelNotes
 * Description: supprimes les enregistrements pour un CONO,FACI, PLGR donné.
 * Date                         Changed By                         Description
 * 20250506                     ddecosterd@hetic3.fr     	création
 */
public class DelNotes extends ExtendM3Transaction {
	private final MIAPI mi;
	private final DatabaseAPI database;

	public DelNotes(MIAPI mi, DatabaseAPI database) {
		this.mi = mi;
		this.database = database;
	}

	public void main() {
		Integer cono = mi.in.get("CONO");
		String  faci = (mi.inData.get("FACI") == null) ? "" : mi.inData.get("FACI").trim();
		String plgr = (mi.inData.get("PLGR") == null) ? "" : mi.inData.get("PLGR").trim();
		int nbKey = 2;

		DBAction exttr1Record = database.table("EXTTR1").index("00").build();
		DBContainer exttr1Container = exttr1Record.createContainer();
		exttr1Container.setInt("EXCONO", cono);
		exttr1Container.setString("EXFACI", faci);
		if(plgr != "") {
			exttr1Container.setString("EXPLGR", plgr);
			nbKey++;
		}
		int read = exttr1Record.readAll(exttr1Container, nbKey, 10000, { DBContainer exttr1Data ->
			DBAction updateRecord = database.table("EXTTR1").index("00").build();
			DBContainer updateContainer = updateRecord.createContainer();
			updateContainer.setInt("EXCONO", exttr1Data.getInt("EXCONO"));
			updateContainer.setString("EXFACI", exttr1Data.getString("EXFACI"));
			updateContainer.setString("EXPLGR", exttr1Data.getString("EXPLGR"));
			updateContainer.setInt("EXOPNO", exttr1Data.getInt("EXOPNO"));
			updateContainer.setLong("EXMERE", exttr1Data.getLong("EXMERE"));
			updateContainer.setString("EXPRNO", exttr1Data.getString("EXPRNO"));
			updateContainer.setString("EXMFNO", exttr1Data.getString("EXMFNO"));
			updateRecord.readLock(updateContainer, { LockedResult result ->
				result.delete();
			});
		});
		mi.getOutData().put("NDEL", read.toString());
	}
}
