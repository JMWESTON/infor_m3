/* Description: Vide les tables EXTTR1 et met à 0 le timestamp de dernière mise à jour pour IPRD002
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

		DBAction extparRecord = database.table("EXTPAR").index("00").selection("EXA015", "EXN018").build();
		DBContainer extparContainer = extparRecord.createContainer();
		extparContainer.setInt("EXCONO", CONO);
		extparContainer.setString("EXFILE", "EXT004");
		extparContainer.setString("EXPK01", "FACI");
		
		if(!extparRecord.read(extparContainer))
			return;

		String  FACI = extparContainer.getString("EXA015");

		extparContainer.setString("EXPK01", "lastUpdate");
		extparContainer.setString("EXPK02", FACI);
		extparRecord.readAllLock(extparContainer, 4, {LockedResult updatedRecord ->
			updatedRecord.setLong("EXN018", 0);
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
