/****************************************************************************************
 Extension Name: EXT007MI/DelMere
 Type: ExtendM3Transaction
 Script Author: d.decosterd@hetic3.fr
 Date: 2025-11-20
 Description:
 * Delete "note mere" from EXTMA1 and EXTMA2
 Revision History:
 Name                    Date             Version          Description of Changes
 d.decosterd@hetic3.fr   2025-11-20       1.0              Creation
 ******************************************************************************************/
public class DelMere extends ExtendM3Transaction {
	private final MIAPI mi;
	private final DatabaseAPI database;

	/*
	 * Transaction  EXT007MI/DelMere Interface
	 * @param mi - Infor MI Interface
	 * @param database - Infor Database Interface
	 */
	public DelMere(MIAPI mi, DatabaseAPI database) {
		this.mi = mi;
		this.database = database;
	}

	public void main() {
		Integer cono = mi.in.get("CONO");
		String  faci = (mi.inData.get("FACI") == null) ? "" : mi.inData.get("FACI").trim();
		Long mere = mi.in.get("MERE");

		DBAction exmat2Record = database.table("EXTMA2").index("20").selection("EXPLGR","EXOPNO","EXMTNO").build();
		DBContainer exmat2Container = exmat2Record.createContainer();
		exmat2Container.setInt("EXCONO", cono);
		exmat2Container.setString("EXFACI", faci);
		exmat2Container.setLong("EXMERE", mere);

		exmat2Record.readAll(exmat2Container, 3, 500, { DBContainer extma2Data ->
			DBAction extma2DelRecord = database.table("EXTMA2").index("00").build();
			DBContainer extma2DelContainer = extma2DelRecord.createContainer();
			extma2DelContainer.setInt("EXCONO", cono);
			extma2DelContainer.setString("EXFACI", faci);
			extma2DelContainer.setLong("EXMERE", mere);
			extma2DelContainer.setString("EXPLGR",extma2Data.getString("EXPLGR"));
			extma2DelContainer.setInt("EXOPNO", extma2Data.getInt("EXOPNO"));
			extma2DelContainer.setString("EXMTNO", extma2Data.getString("EXMTNO"));

			extma2DelRecord.readLock(extma2DelContainer, {  LockedResult result ->
				result.delete(); });
		});

		DBAction extma1Record = database.table("EXTMA1").index("10").selection("EXPLGR","EXOPNO").build();
		DBContainer extma1Container = extma1Record.createContainer();
		extma1Container.setInt("EXCONO", cono);
		extma1Container.setString("EXFACI", faci);
		extma1Container.setLong("EXMERE", mere);
		int read = extma1Record.readAll(extma1Container, 3, 500, { DBContainer extma1Data ->
			DBAction extma1DelRecord = database.table("EXTMA1").index("00").build();
			DBContainer extma1DelContainer = extma1DelRecord.createContainer();
			extma1DelContainer.setInt("EXCONO", cono);
			extma1DelContainer.setString("EXFACI", faci);
			extma1DelContainer.setLong("EXMERE", mere);
			extma1DelContainer.setString("EXPLGR",extma1Data.getString("EXPLGR"));
			extma1DelContainer.setInt("EXOPNO", extma1Data.getInt("EXOPNO"));

			extma1DelRecord.readLock(extma1DelContainer, {  LockedResult result ->
				result.delete(); });
		});

		if(read == 0) {
			mi.error("Enregistrement inexistant.");
			return;
		}
	}
}