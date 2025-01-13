import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * README
 *
 * Name: EXT001
 * Description: Calcul des matériaux par note mère
 * Date                         Changed By                         Description
 * 20240702                     ddecosterd@hetic3.fr     	création
 */
public class EXT001 extends ExtendM3Batch {
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;

	public EXT001(ProgramAPI program, DatabaseAPI database, UtilityAPI utility) {
		this.program = program;
		this.database = database;
		this.utility = utility;
	}

	public void main() {
		Integer CONO = program.getLDAZD().CONO;
		String  FACI = "FF1";

		String tableName = getTableName(CONO);

		DBAction tr2Record = database.table(tableName).index("00").build();
		DBContainer tr2Container = tr2Record.createContainer();
		tr2Container.setString("EXBJNO", "MATERIAL_CALC");
		tr2Record.readAllLock(tr2Container,1,{LockedResult entry ->
			entry.delete();
		});

		DBAction mwohedRecord = database.table("MWOHED").index("95").selection("VHMFNO","VHPRNO","VHSCHN").build();
		DBContainer mwohedContainer = mwohedRecord.createContainer();
		mwohedContainer.setInt("VHCONO", CONO);
		mwohedContainer.setString("VHFACI", FACI);

		mwohedRecord.readAll( mwohedContainer, 2, { DBContainer mwohedData ->
			ExpressionFactory mwomatExpression = database.getExpressionFactory("MWOMAT");
			mwomatExpression = mwomatExpression.eq("VMSPMT", "2");
			DBAction mwomatRecord = database.table("MWOMAT").index("10").matching(mwomatExpression).selection("VMOPNO","VMMTNO","VMREQT","VMRPQT","VMPLGR").build();
			DBContainer mwomatContainer = mwomatRecord.createContainer();
			mwomatContainer.setInt("VMCONO", CONO);
			mwomatContainer.setString("VMFACI", FACI);
			mwomatContainer.setString("VMPRNO", mwohedData.getString("VHPRNO"));
			mwomatContainer.setString("VMMFNO",mwohedData.getString("VHMFNO"));
			mwomatRecord.readAll(mwomatContainer, 4, 1000,{ DBContainer mwomatData ->
				DBAction exttr2Record = database.table(tableName).index("00").build();
				DBContainer exttr2Container = exttr2Record.createContainer();
				exttr2Container.setString("EXBJNO", "MATERIAL_CALC");
				exttr2Container.setInt("EXCONO", CONO);
				exttr2Container.setString("EXFACI", FACI);
				exttr2Container.setString("EXPLGR", mwomatData.getString("VMPLGR"));
				exttr2Container.set("EXMERE", mwohedData.get("VHSCHN"));
				exttr2Container.setInt("EXOPNO", mwomatData.getInt("VMOPNO"))
				exttr2Container.setString("EXMTNO", mwomatData.getString("VMMTNO"));

				if(!exttr2Record.readLock(exttr2Container, {LockedResult updatedRecord ->
							updatedRecord.setDouble("EXREQT", updatedRecord.getDouble("EXREQT") + mwomatData.getDouble("VMREQT"));
							updatedRecord.setDouble("EXRPQT", updatedRecord.getDouble("EXRPQT") + mwomatData.getDouble("VMRPQT"));
							updateTrackingField(updatedRecord, "EX");
							updatedRecord.update();
						})) {
					exttr2Container.setDouble("EXREQT", mwomatData.getDouble("VMREQT"));
					exttr2Container.setDouble("EXRPQT", mwomatData.getDouble("VMRPQT"));
					insertTrackingField(exttr2Container, "EX");
					exttr2Record.insert(exttr2Container);
				}
			});
		});
		saveTableName(CONO, tableName);
	}

	/**
	 * Get table name to use for this call.
	 * @param cono
	 * @return The table name.
	 */
	private String getTableName(Integer cono) {
		String tableName = "EXTTR2";
		DBAction extparRecord = database.table("EXTPAR").index("00").selection("EXA015").build();
		DBContainer extparContainer = extparRecord.createContainer();
		extparContainer.setInt("EXCONO", cono);
		extparContainer.setString("EXFILE", "EXT001");
		extparContainer.setString("EXPK01", "sourceTable");

		boolean foundRecord = extparRecord.read(extparContainer);
		if( foundRecord) {
			if (extparContainer.getString("EXA015").equals("EXTTR2") )
				tableName = "EXTTR3";
			else
				tableName = "EXTTR2";

		}else {
			extparContainer.setString("EXA015", "EXTTR2")
			insertTrackingField(extparContainer,"EX");
			extparRecord.insert(extparContainer);
		}
		return tableName;

	}

	/**
	 * Update the table to read in EXT005MI.LstNotes
	 * @param cono
	 * @param tableName
	 */
	private void saveTableName(Integer cono, String tableName) {
		DBAction extparRecord = database.table("EXTPAR").index("00").build();
		DBContainer extparContainer = extparRecord.createContainer();
		extparContainer.setInt("EXCONO", cono);
		extparContainer.setString("EXFILE", "EXT001");
		extparContainer.setString("EXPK01", "sourceTable");

		extparRecord.readLock(extparContainer,{LockedResult updatedRecord ->
			updatedRecord.setString("EXA015",tableName);
			updatedRecord.setString("EXA115", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd HHmmss")));
			updateTrackingField(updatedRecord, "EX");
			updatedRecord.update();
		});

	}

	/**
	 *  Add default value for new record.
	 * @param insertedRecord
	 * @param prefix The column prefix of the table.
	 */
	private void insertTrackingField(DBContainer insertedRecord, String prefix) {
		insertedRecord.set(prefix+"RGDT", (Integer) utility.call("DateUtil", "currentDateY8AsInt"));
		insertedRecord.set(prefix+"LMDT", (Integer) utility.call("DateUtil", "currentDateY8AsInt"));
		insertedRecord.set(prefix+"CHID", program.getUser());
		insertedRecord.set(prefix+"RGTM", (Integer) utility.call("DateUtil", "currentTimeAsInt"));
		insertedRecord.set(prefix+"CHNO", 1);
	}

	/**
	 *  Add default value for updated record.
	 * @param updatedRecord
	 * @param prefix The column prefix of the table.
	 */
	private void updateTrackingField(LockedResult updatedRecord, String prefix) {
		int CHNO = updatedRecord.getInt(prefix+"CHNO");
		if(CHNO== 999) {CHNO = 0;}
		CHNO++;
		updatedRecord.set(prefix+"LMDT", (Integer) utility.call("DateUtil", "currentDateY8AsInt"));
		updatedRecord.set(prefix+"CHID", program.getUser());
		updatedRecord.setInt(prefix+"CHNO", CHNO);
	}
}
