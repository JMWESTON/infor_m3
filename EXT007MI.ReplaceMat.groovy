/**
 * README
 *
 * Name: EXT007MI.ReplaceMat
 * Description: Met à jour la quantité du matériau dans la table extma2 suite à un remplacement de matériau
 * Date                         Changed By                    Description
 * 20250213                     ddecosterd@hetic3.fr     		création
 */
public class ReplaceMat extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;
	private final MICallerAPI miCaller;

	public ReplaceMat(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility, MICallerAPI miCaller) {
		this.mi = mi;
		this.program = program;
		this.database = database;
		this.utility = utility;
		this.miCaller = miCaller;
	}

	public void main() {
		Integer CONO = mi.in.get("CONO");
		String  FACI = (mi.inData.get("FACI") == null) ? "" : mi.inData.get("FACI").trim();
		String PRNO = (mi.inData.get("PRNO") == null) ? "" : mi.inData.get("PRNO").trim();
		String MFNO = (mi.inData.get("MFNO") == null) ? "" : mi.inData.get("MFNO").trim();
		String PLGR = (mi.inData.get("PLGR") == null) ? "" : mi.inData.get("PLGR").trim();
		Long MERE = mi.in.get("MERE");
		Integer OPNO = mi.in.get("OPNO");
		String OTNO = (mi.inData.get("OTNO") == null) ? "" : mi.inData.get("OTNO").trim();
		String NTNO = (mi.inData.get("NTNO") == null) ? "" : mi.inData.get("NTNO").trim();
		Double REQT = mi.in.get("REQT");
		Integer SPMT = mi.in.get("SPMT");

		if(!checkInputs(CONO, FACI, PRNO, MFNO, PLGR, MERE, OPNO, OTNO, NTNO, REQT)) {
			return;
		}

		DBAction extma2Record = database.table("EXTMA2").index("00").selection("EXREQT","EXCHNO").build();
		DBContainer extma2Container = extma2Record.createContainer();
		extma2Container.setInt("EXCONO", CONO);
		extma2Container.setString("EXFACI", FACI);
		extma2Container.setString("EXPLGR",PLGR);
		extma2Container.setLong("EXMERE", MERE);
		extma2Container.setInt("EXOPNO", OPNO);
		extma2Container.setString("EXMTNO", OTNO);

		int delete = 0;

		extma2Record.readLock(extma2Container, { LockedResult lockedRecord ->
			double newQty = lockedRecord.getDouble("EXREQT") - REQT;
			ExpressionFactory mwomatExpressionFactory = database.getExpressionFactory("MWOMAT");
			mwomatExpressionFactory = mwomatExpressionFactory.eq("VMMTNO", OTNO);
			DBAction mwomatRecord = database.table("MWOMAT").index("10").matching(mwomatExpressionFactory).build();
			DBContainer mwomatContainer = mwomatRecord.createContainer();
			mwomatContainer.setInt("VMCONO", CONO);
			mwomatContainer.setString("VMFACI",FACI);
			mwomatContainer.setString("VMPRNO", PRNO);
			mwomatContainer.setString("VMMFNO", MFNO);
			mwomatContainer.setInt("VMOPNO", OPNO);
			int read  = mwomatRecord.readAll(mwomatContainer, 5, 1, {});
			if(read == 0) {
				lockedRecord.delete();
				delete = 1;
			}
			else {
				lockedRecord.setDouble("EXREQT", newQty);
				updateTrackingField(lockedRecord, "EX");
				lockedRecord.update();

			}
		});

		extma2Container.setString("EXMTNO", NTNO);
		if(!extma2Record.readLock(extma2Container, { LockedResult updatedRecord ->
					updatedRecord.setDouble("EXREQT", REQT + updatedRecord.getDouble("EXREQT"));
					updateTrackingField(updatedRecord, "EX");
					updatedRecord.update();
				})){
			extma2Container.setDouble("EXREQT", REQT);
			extma2Container.setInt("EXSPMT", SPMT);
			insertTrackingField(extma2Container, "EX");
			extma2Record.insert(extma2Container);
		}

		Map<String,String> parameters =  ["CONO":CONO.toString(),FACI:FACI,MERE:MERE.toString(),INDX:"10",PLGR:PLGR,OPNO:OPNO.toString(),NDEL:delete.toString()];
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
	 * @param plgr
	 * @param mere
	 * @param opno
	 * @param otno
	 * @param ntno
	 * @param reqt
	 * @return true if no error.
	 */
	private boolean checkInputs(Integer cono, String  faci, String prno, String mfno, String plgr, Long mere, Integer opno, String otno, String ntno, Double reqt ) {
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

		if(prno == null) {
			mi.error("Le produit est obligatoire");
			return false;
		}

		if(mfno == null) {
			mi.error("Le numéro d'OF est obligatoire");
			return false;
		}

		if(opno == null || opno == 0) {
			mi.error("Le numéro d'opération est obligatoire.");
			return false;
		}

		if(otno == null || otno.isEmpty()) {
			mi.error("Ancien numéro de composant est obligatoire.");
			return false;
		}

		if(ntno == null || ntno.isEmpty()) {
			mi.error("Nouveau numéro de composant est obligatoire.");
			return false;
		}

		if(reqt == null) {
			mi.error("Laquantité réservée est obligatoire.");
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
