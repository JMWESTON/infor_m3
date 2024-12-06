/* Description: Vide les tables EXTTR1 et EXTTR2 et met à 0 le timestamp de dernière mise à jour pour IPRD002
 * Date                         Changed By                         Description
 * 20241016                     ddecosterd@hetic3.fr     	création
 */
public class EXT004 extends ExtendM3Batch {
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;

	public EXT004(ProgramAPI program, DatabaseAPI database, UtilityAPI utility) {
		this.program = program;
		this.database = database;
		this.utility = utility;
	}

	public void main() {
		Integer CONO = program.getLDAZD().CONO;

		DBAction tr1Record = database.table("EXTTR1").index("00").build();
		DBContainer tr1Container = tr1Record.createContainer();
		tr1Record.readAllLock(tr1Container,0,{LockedResult entry ->
			entry.delete();
		});

		DBAction tr2Record = database.table("EXTTR2").index("00").build();
		DBContainer tr2Container = tr2Record.createContainer();
		tr2Record.readAllLock(tr2Container,0,{LockedResult entry ->
			entry.delete();
		});

		DBAction CUGEX1Record = database.table("CUGEX1").index("00").selection("F1A030").build();
		DBContainer CUGEX1Container = CUGEX1Record.createContainer();
		CUGEX1Container.setInt("F1CONO", CONO);
		CUGEX1Container.setString("F1FILE", "BATCH");
		CUGEX1Container.setString("F1PK01", "EXT004");

		if(!CUGEX1Record.read(CUGEX1Container))
			return;

		String  FACI = CUGEX1Container.getString("F1A030");

		CUGEX1Record = database.table("CUGEX1").index("00").selection("F1N096").build();
		CUGEX1Container = CUGEX1Record.createContainer();
		CUGEX1Container.setInt("F1CONO", CONO);
		CUGEX1Container.setString("F1FILE", "EXTEND");
		CUGEX1Container.setString("F1PK01", "IPRD002");
		CUGEX1Container.setString("F1PK02", FACI);
		CUGEX1Record.readAllLock(CUGEX1Container, 4, {LockedResult updatedRecord ->
			updatedRecord.setDouble("F1N096", 0);
			updateTrackingField(updatedRecord, "F1");
			updatedRecord.update();
		});

	}

	private void updateTrackingField(LockedResult updatedRecord, String prefix) {
		int CHNO = updatedRecord.getInt(prefix+"CHNO");
		if(CHNO== 999) {CHNO = 0;}
		updatedRecord.set(prefix+"LMDT", (Integer) this.utility.call("DateUtil", "currentDateY8AsInt"));
		updatedRecord.set(prefix+"CHID", this.program.getUser());
		updatedRecord.setInt(prefix+"CHNO", CHNO);
	}

}
