/**
 * README
 *
 * Name: EXT007MI.DelMat
 * Description: Supprime le matériau dans la table exmat2
 * Date                         Changed By                    Description
 * 20250130                     ddecosterd@hetic3.fr     		création
 */
public class DelMat extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;

	public DelMat(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility) {
		this.mi = mi;
		this.program = program;
		this.database = database;
		this.utility = utility;
	}

	public void main() {
		Integer CONO = mi.in.get("CONO");
		String  FACI = (mi.inData.get("FACI") == null) ? "" : mi.inData.get("FACI").trim();
		String PLGR = (mi.inData.get("PLGR") == null) ? "" : mi.inData.get("PLGR").trim();
		Long MERE = mi.in.get("MERE");
		Integer OPNO = mi.in.get("OPNO");
		String MTNO = (mi.inData.get("MTNO") == null) ? "" : mi.inData.get("MTNO").trim();
		Double REQT = mi.in.get("REQT");
		String WHST = (mi.inData.get("WHST") == null) ? "" : mi.inData.get("WHST").trim();

		if(!checkInputs(CONO, FACI, PLGR, MERE, OPNO, MTNO)) {
			return;
		}

		DBAction exmat2Record = database.table("EXTMA2").index("00").selection("EXREQT").build();
		DBContainer exmat2Container = exmat2Record.createContainer();
		exmat2Container.setInt("EXCONO", CONO);
		exmat2Container.setString("EXFACI", FACI);
		exmat2Container.setString("EXPLGR",PLGR);
		exmat2Container.setLong("EXMERE", MERE);
		exmat2Container.setInt("EXOPNO", OPNO);
		exmat2Container.setString("EXMTNO", MTNO);

		if(WHST > "90")
			return;

		boolean found = exmat2Record.readLock(exmat2Container, { LockedResult updatedRecord ->
			double newQty = updatedRecord.getDouble("EXREQT") - REQT;
			if(newQty <= 0 ) {
				updatedRecord.delete();
			}else {
				updatedRecord.setDouble("EXREQT", newQty);
				updateTrackingField(updatedRecord, "EX");
				updatedRecord.update();
			}
		});

		if(!found) {
			mi.error("L'enregistrement n'existe pas.");
			return;
		}

		DBAction exmat210Record = database.table("EXTMA2").index("10").selection("EXMTNO","EXREQT").build();
		DBContainer exmat210Container = exmat210Record.createContainer();
		exmat210Container.setInt("EXCONO", CONO);
		exmat210Container.setString("EXFACI", FACI);
		exmat210Container.setString("EXPLGR",PLGR);
		exmat210Container.setLong("EXMERE", MERE);
		exmat210Container.setInt("EXOPNO", OPNO);

		int nbMat = 1;
		String exmnt1 = "";
		exmat210Record.readAll(exmat210Container, 5, 1000, {  DBContainer extma2Data ->
			DBAction extma1Record = database.table("EXTMA1").index("00").build();
			DBContainer extma1Container = extma1Record.createContainer();
			extma1Container.setInt("EXCONO", extma2Data.getInt("EXCONO"));
			extma1Container.setString("EXFACI", extma2Data.getString("EXFACI"));
			extma1Container.setString("EXPLGR", extma2Data.getString("EXPLGR"));
			extma1Container.setLong("EXMERE", extma2Data.getLong("EXMERE"));
			extma1Container.setInt("EXOPNO",extma2Data.getInt("EXOPNO"));
			if(nbMat == 1) {
				exmnt1 = extma2Data.getString("EXMTNO");
				extma1Container.setString("EXMTN1", exmnt1);
				if(!extma1Record.readLock(extma1Container, { LockedResult updatedRecord ->
							updatedRecord.setDouble("EXRQT1", updatedRecord.getDouble("EXRQT1") + extma2Data.getDouble("EXREQT"))
							updateTrackingField(updatedRecord, "EX");
							updatedRecord.update();
						})){
					extma1Container.setDouble("EXRQT1", extma2Data.getDouble("EXREQT"));
					insertTrackingField(extma1Container, "EX");
					extma1Record.insert(extma1Container);
				}
			}else
				if(nbMat == 2) {
					extma1Container.setString("EXMTN1", exmnt1);
					extma1Record.readLock(extma1Container, { LockedResult updatedRecord ->
						updatedRecord.setString("EXMTN2", extma2Data.getString("EXMTNO"));
						updatedRecord.setDouble("EXRQT2", updatedRecord.getDouble("EXRQT2") + extma2Data.getDouble("EXREQT"))
						updateTrackingField(updatedRecord, "EX");
						updatedRecord.update();
					});
				}else {
					extma1Container.setInt("EXNBMA", nbMat);
					extma1Record.readLock(extma1Container, { LockedResult updatedRecord ->
						updatedRecord.setString("EXMTN2", extma2Data.getString("EXMTNO"));
						updatedRecord.setDouble("EXRQT2", updatedRecord.getDouble("EXRQT2") + extma2Data.getDouble("EXREQT"))
						updateTrackingField(updatedRecord, "EX");
						updatedRecord.update();
					});
				}

			nbMat++;
		});

	}

	/**
	 * Check input values
	 * @param cono
	 * @param faci
	 * @param plgr
	 * @param mere
	 * @param opno
	 * @param mtno
	 * @return true if no error.
	 */
	private boolean checkInputs(Integer cono, String  faci, String plgr, Long mere, Integer opno, String mtno ) {
		if(cono == null) {
			mi.error("La division est obligatoire.");
			return false;
		}
		if(!utility.call("CheckUtil", "checkConoExist", database, cono)) {
			mi.error("La division est inexistante.");
			return false;
		}

		if(faci.isEmpty()) {
			mi.error("L'établissement est obligatoire.");
			return false;
		}
		if(!utility.call("CheckUtil", "checkFacilityExist", database, cono, faci)) {
			mi.error("L'établissement est inexistant.");
			return false;
		}

		if(plgr == null) {
			mi.error("Le poste de charge est obligatoire.");
			return false;
		}
		if(!utility.call("CheckUtil", "checkPLGRExist", database, cono, faci, plgr)) {
			mi.error("Le poste de charge est inexistant");
			return false;
		}

		if(mere == null) {
			mi.error("La note mère est obligatoire");
			return false;
		}
		if(!utility.call("CheckUtil", "checkSCHNExist", database, cono, mere)){
			mi.error("La note mère n'existe pas.");
			return false;
		}

		if(opno == null || opno == 0) {
			mi.error("Le numéro d'opération est obligatoire.");
			return false;
		}

		if(mtno == null || mtno.isEmpty()) {
			mi.error("Numéro de composant est obligatoire.");
			return false;
		}

		return true;
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
