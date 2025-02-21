/**
 * README
 *
 * Name: EXT007MI.AgregMat
 * Description: Met à jour les matériaux et leur quantités dans la table extma1
 * Date                         Changed By                    Description
 * 20250203                     ddecosterd@hetic3.fr     		création
 */
public class AgregMat extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;

	public AgregMat(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility) {
		this.mi = mi;
		this.program = program;
		this.database = database;
		this.utility = utility;
	}

	public void main() {
		Integer CONO = mi.in.get("CONO");
		String  FACI = (mi.inData.get("FACI") == null) ? "" : mi.inData.get("FACI").trim();
		Long MERE = mi.in.get("MERE");
		String INDX = (mi.inData.get("INDX") == null) ? "" : mi.inData.get("INDX").trim();
		String PLGR = (mi.inData.get("PLGR") == null) ? "" : mi.inData.get("PLGR").trim();
		Integer OPNO = mi.in.get("OPNO");
		Integer NDEL = mi.in.get("NDEL");

		if(!checkInputs(CONO, FACI, MERE, INDX, PLGR, OPNO, NDEL)) {
			return;
		}
		DBAction exmat2Record;
		DBContainer exmat2Container;
		int nbKeys;

		if(INDX == "10") {
			exmat2Record = database.table("EXTMA2").index("10").selection("EXMTNO","EXREQT","EXCHNO","EXSPMT").build();
			exmat2Container = exmat2Record.createContainer();
			exmat2Container.setInt("EXCONO", CONO);
			exmat2Container.setString("EXFACI", FACI);
			exmat2Container.setString("EXPLGR",PLGR);
			exmat2Container.setLong("EXMERE", MERE);
			exmat2Container.setInt("EXOPNO", OPNO);
			nbKeys = 5;
		}else {
			exmat2Record = database.table("EXTMA2").index("20").selection("EXPLGR","EXOPNO","EXMTNO","EXREQT","EXCHNO","EXSPMT").build();
			exmat2Container = exmat2Record.createContainer();
			exmat2Container.setInt("EXCONO", CONO);
			exmat2Container.setString("EXFACI", FACI);
			exmat2Container.setLong("EXMERE", MERE);
			nbKeys = 3;
		}

		if(NDEL == 1) {
			DBAction extma1Record = database.table("EXTMA1").index("00").selection("EXPLGR","EXOPNO","EXCHNO").build();
			DBContainer extma1Container = extma1Record.createContainer();
			extma1Container.setInt("EXCONO", CONO);
			extma1Container.setString("EXFACI", FACI);
			extma1Container.setLong("EXMERE", MERE);
			if(INDX == "10") {
				extma1Container.setString("EXPLGR",PLGR);
				extma1Container.setInt("EXOPNO", OPNO);
			}
			if(INDX == "10") {
				extma1Record.readLock(extma1Container, { LockedResult result ->
					result.delete();
				});
			}else {
				extma1Record.readAll(extma1Container, 3,1000, { DBContainer extma1Data ->
					DBAction extma1DelRecord = database.table("EXTMA1").index("00").build();
					DBContainer extma1DelContainer = extma1DelRecord.createContainer();
					extma1DelContainer.setInt("EXCONO", CONO);
					extma1DelContainer.setString("EXFACI", FACI);
					extma1DelContainer.setLong("EXMERE", MERE);
					extma1DelContainer.setString("EXPLGR",extma1Data.getString("EXPLGR"));
					extma1DelContainer.setInt("EXOPNO", extma1Data.getInt("EXOPNO"));

					extma1DelRecord.readLock(extma1DelContainer, {  LockedResult result ->
						result.delete(); });
				});
			}
		}


		int nbMat = 1;
		exmat2Record.readAll(exmat2Container, nbKeys, 100, {  DBContainer extma2Data ->
			DBAction extma1Record = database.table("EXTMA1").index("00").build();
			DBContainer extma1Container = extma1Record.createContainer();
			extma1Container.setInt("EXCONO", extma2Data.getInt("EXCONO"));
			extma1Container.setString("EXFACI", extma2Data.getString("EXFACI"));
			extma1Container.setString("EXPLGR", extma2Data.getString("EXPLGR"));
			extma1Container.setLong("EXMERE", extma2Data.getLong("EXMERE"));
			extma1Container.setInt("EXOPNO",extma2Data.getInt("EXOPNO"));

			if(extma2Data.getInt("EXSPMT") == 2) {
				if(nbMat == 1) {
					if(!extma1Record.readLock(extma1Container, { LockedResult updatedRecord ->
								updatedRecord.setString("EXMTN1", extma2Data.getString("EXMTNO"));
								updatedRecord.setDouble("EXRQT1", extma2Data.getDouble("EXREQT"));
								updatedRecord.setInt("EXNBMA", nbMat);
								updateTrackingField(updatedRecord, "EX");
								updatedRecord.update();
							})){
						extma1Container.setString("EXMTN1", extma2Data.getString("EXMTNO"));
						extma1Container.setDouble("EXRQT1", extma2Data.getDouble("EXREQT"));
						extma1Container.setString("EXPLG2", "S_"+PLGR.substring(2));
						extma1Container.setInt("EXNBMA", nbMat);
						insertTrackingField(extma1Container, "EX");
						extma1Record.insert(extma1Container);
					}
				}else
					if(nbMat == 2) {
						extma1Record.readLock(extma1Container, { LockedResult updatedRecord ->
							updatedRecord.setString("EXMTN2", extma2Data.getString("EXMTNO"));
							updatedRecord.setDouble("EXRQT2", extma2Data.getDouble("EXREQT"));
							updatedRecord.setInt("EXNBMA", nbMat);
							updateTrackingField(updatedRecord, "EX");
							updatedRecord.update();
						});
					}else {
						extma1Record.readLock(extma1Container, { LockedResult updatedRecord ->
							updatedRecord.setInt("EXNBMA", nbMat);
							updateTrackingField(updatedRecord, "EX");
							updatedRecord.update();
						});
					}
				nbMat++;
			}else {
				if(!extma1Record.read(extma1Container)){
					extma1Container.setString("EXMTN1", extma2Data.getString("EXMTNO"));
					extma1Container.setDouble("EXRQT1", extma2Data.getDouble("EXREQT"));
					extma1Container.setString("EXPLG2", "S_"+PLGR.substring(2));
					extma1Container.setInt("EXNBMA", 0);
					insertTrackingField(extma1Container, "EX");
					extma1Record.insert(extma1Container);
				}

			}
		});
	}
	/**
	 * Check input values
	 * @param cono
	 * @param faci
	 * @param plgr
	 * @param mere
	 * @param opno
	 * @param ndel
	 * @return true if no error.
	 */
	private boolean checkInputs(Integer cono, String  faci, Long mere, String indx, String plgr, Integer opno, Integer ndel ) {
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

		if(mere == null) {
			mi.error("La note mère est obligatoire");
			return false;
		}
		if(!utility.call("CheckUtil", "checkSCHNExist", database, cono, mere)) {
			mi.error("La note mère est inexistante.")
			return false;
		}

		if(indx.isEmpty()) {
			mi.error("L'index est obligatoire.");
			return false;
		}
		if(indx != "10" && indx != "20") {
			mi.error("Valeur index incorrect.");
			return false;
		}

		if(indx == "10") {
			if(plgr == null) {
				mi.error("Le poste de charge est obligatoire en index 10.");
				return false;
			}
			if(!utility.call("CheckUtil", "checkPLGRExist", database, cono, faci, plgr)) {
				mi.error("Le poste de charge est inexistant.");
				return false;
			}

			if(opno == null || opno == 0) {
				mi.error("Le numéro d'opération est obligatoire en index 10.");
				return false;
			}
		}

		if(ndel != null && (ndel <0 || ndel > 1)) {
			mi.error("Need deletion doit être compris entre 0 et 1.");
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
