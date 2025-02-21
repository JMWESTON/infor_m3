/**
 * README
 *
 * Name: EXT007MI.ReplaceSCHN
 * Description: Met à jour la quantité du matériau dans la table extma2 suite à un changement de schn sur l'OF
 * Date                         Changed By                    Description
 * 20250217                     ddecosterd@hetic3.fr     		création
 */
public class ReplaceSCHN extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;
	private final MICallerAPI miCaller;

	public ReplaceSCHN(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility, MICallerAPI miCaller) {
		this.mi = mi;
		this.program = program;
		this.database = database;
		this.utility = utility;
		this.miCaller = miCaller;
	}

	public void main() {
		Integer CONO = mi.in.get("CONO");
		String  FACI = (mi.inData.get("FACI") == null) ? "" : mi.inData.get("FACI").trim();
		Long OCHN = mi.in.get("OCHN");
		Long NCHN = mi.in.get("NCHN");
		String PRNO = (mi.inData.get("PRNO") == null) ? "" : mi.inData.get("PRNO").trim();
		String MFNO = (mi.inData.get("MFNO") == null) ? "" : mi.inData.get("MFNO").trim();

		if(OCHN == 0)
			return;

		if(!checkInputs(CONO, FACI, OCHN, NCHN, PRNO, MFNO)) {
			return;
		}

		ExpressionFactory mwomatExpressionFactory = database.getExpressionFactory("MWOMAT");
		mwomatExpressionFactory = mwomatExpressionFactory.eq("VMSPMT", "2");
		DBAction mwomatRecord = database.table("MWOMAT").index("10").matching(mwomatExpressionFactory).selection("VMPLGR","VMOPNO","VMMTNO","VMREQT","VMSPMT").build();
		DBContainer mwomatContainer = mwomatRecord.createContainer();
		mwomatContainer.setInt("VMCONO", CONO);
		mwomatContainer.setString("VMFACI",FACI);
		mwomatContainer.setString("VMPRNO", PRNO);
		mwomatContainer.setString("VMMFNO", MFNO);

		mwomatRecord.readAll(mwomatContainer, 4, 1000,{ DBContainer mwomatData ->
			DBAction extma2Record = database.table("EXTMA2").index("00").selection("EXREQT").build();
			DBContainer extma2Container = extma2Record.createContainer();
			extma2Container.setInt("EXCONO", CONO);
			extma2Container.setString("EXFACI", FACI);
			extma2Container.setString("EXPLGR", mwomatData.getString("VMPLGR"));
			extma2Container.setLong("EXMERE", OCHN);
			extma2Container.setInt("EXOPNO", mwomatData.getInt("VMOPNO"));
			extma2Container.setString("EXMTNO", mwomatData.getString("VMMTNO"));
			extma2Record.readLock(extma2Container, { LockedResult updatedRecord ->
				updatedRecord.setDouble("EXREQT", updatedRecord.getDouble("EXREQT") - mwomatData.getDouble("VMREQT"));
				updateTrackingField(updatedRecord, "EX");
				updatedRecord.update();
			});

			extma2Container.setLong("EXMERE", OCHN);
			if(!extma2Record.readLock(extma2Container, { LockedResult updatedRecord ->
						updatedRecord.setDouble("EXREQT",  mwomatData.getDouble("VMREQT") + updatedRecord.getDouble("EXREQT"));
						updateTrackingField(updatedRecord, "EX");
						updatedRecord.update();
					})){
				extma2Container.setDouble("EXREQT",  mwomatData.getDouble("VMREQT"));
				extma2Container.setInt("EXSPMT", mwomatData.getInt("VMSPMT"));
				insertTrackingField(extma2Container, "EX");
				extma2Record.insert(extma2Container);
			}

		});
		Map<String,String> parameters =  ["CONO":CONO.toString(),FACI:FACI,MERE:OCHN.toString(),INDX:"20",NDEL:"1"];
		miCaller.call("EXT007MI", "AgregMat", parameters , { Map<String, String> response ->
			if(response.error) {
				mi.error(response.errorMessage);
			}
		});

		parameters =  ["CONO":CONO.toString(),FACI:FACI,MERE:NCHN.toString(),INDX:"20",NDEL:"1"];
		miCaller.call("EXT007MI", "AgregMat", parameters , { Map<String, String> response ->
			if(response.error) {
				mi.error(response.errorMessage);
			}
		});
	}
	/**
	 * Check input values
	 * @param cono
	 * @param faci
	 * @param ochn
	 * @param nchn
	 * @param prno
	 * @param mfno
	 * @return true if no error.
	 */
	private boolean checkInputs(Integer cono, String  faci, Long ochn, Long nchn, String prno, String mfno ) {
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

		if(ochn == null) {
			mi.error("L'ancien numéro de programme est obligatoire");
			return false;
		}
		if(!utility.call("CheckUtil", "checkSCHNExist", database, cono, ochn)){
			mi.error("L'ancien numéro de programme n'existe pas.");
			return false;
		}
		if(nchn == null) {
			mi.error("Le nouveau numéro de programme est obligatoire");
			return false;
		}
		if(!utility.call("CheckUtil", "checkSCHNExist", database, cono, nchn)){
			mi.error("Le nouveau numéro de programme n'existe pas.");
			return false;
		}

		if(prno == null || prno.isEmpty()) {
			mi.error("Le produit est obligatoire.");
			return false;
		}

		if(mfno == null || mfno.isEmpty()) {
			mi.error("Le numéro d'OF est obligatoire.");
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