/****************************************************************************************
 Extension Name: EXT007MI/LstDeletedMere
 Type: ExtendM3Transaction
 Script Author: d.decosterd@hetic3.fr
 Date: 2026-02-20
 Description:
 * A list of mere in EXTMA1 and not in MWOHED
 Revision History:
 Name                    Date             Version          Description of Changes
 d.decosterd@hetic3.fr   2025-02-20       1.0              Creation
 ******************************************************************************************/
public class LstDeletedMere extends ExtendM3Transaction {
	private final MIAPI mi;
	private final DatabaseAPI database;

	/*
	 * Transaction  EXT007MI/LstDeletedMere Interface
	 * @param mi - Infor MI Interface
	 * @param database - Infor Database Interface
	 */
	public LstDeletedMere(MIAPI mi, DatabaseAPI database) {
		this.mi = mi;
		this.database = database;
	}

	public void main() {
		Integer cono = mi.in.get("CONO");
		String  faci = (mi.inData.get("FACI") == null) ? "" : mi.inData.get("FACI").trim();

		Long mere = 0;
		DBAction extma1Record = database.table("EXTMA1").index("10").selection("EXMERE").build();
		DBContainer extma1Container = extma1Record.createContainer();
		extma1Container.setInt("EXCONO", cono);
		extma1Container.setString("EXFACI", faci);
		int readExtma1 = extma1Record.readAll(extma1Container, 2, 1, { DBContainer extma1Data ->
			mere = extma1Data.getLong("EXMERE");
		});

		int nrOfRecords = 0;
		int nrMaxOfRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000? 10000: mi.getMaxRecords();

		while(readExtma1 != 0 && nrOfRecords < nrMaxOfRecords) {
			DBAction mwohedRecord = database.table("MWOHED").index("65").build();
			DBContainer mwohedContainer = mwohedRecord.createContainer();
			mwohedContainer.setInt("VHCONO", cono);
			mwohedContainer.setString("VHFACI", faci);
			mwohedContainer.setLong("VHSCHN", mere);

			int read = mwohedRecord.readAll(mwohedContainer, 3, 1, {});
			if(read == 0) {
				nrOfRecords ++;
				mi.getOutData().put("CONO", cono.toString());
				mi.getOutData().put("FACI", faci.toString());
				mi.getOutData().put("MERE", mere.toString());
				mi.write();
			}
			ExpressionFactory extmat1ExpressionFactory = database.getExpressionFactory("EXTMA1");
			extmat1ExpressionFactory = extmat1ExpressionFactory.gt("EXMERE", mere.toString());
			extma1Record = database.table("EXTMA1").index("10").matching(extmat1ExpressionFactory).selection("EXMERE").build();
			extma1Container = extma1Record.createContainer();
			extma1Container.setInt("EXCONO", cono);
			extma1Container.setString("EXFACI", faci);
			readExtma1 = extma1Record.readAll(extma1Container, 2, 1, { DBContainer extma1Data ->
				mere = extma1Data.getLong("EXMERE");
			});
		}
	}
}
