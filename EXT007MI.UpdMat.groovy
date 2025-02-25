/**
 * README
 *
 * Name: EXT007MI.UpdMat
 * Description: Met à jour la quantité du matériau dans la table extma2
 * Date                         Changed By                    Description
 * 20250130                     ddecosterd@hetic3.fr     		création
 * 20250221						ddecosterd@hetic3.fr			Add filter on SPMT
 */
public class UpdMat extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;
	private final MICallerAPI miCaller;

	public UpdMat(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility, MICallerAPI miCaller) {
		this.mi = mi;
		this.program = program;
		this.database = database;
		this.utility = utility;
		this.miCaller = miCaller;
	}

	public void main() {
		Integer CONO = mi.in.get("CONO");
		String  FACI = (mi.inData.get("FACI") == null) ? "" : mi.inData.get("FACI").trim();
		String PLGR = (mi.inData.get("PLGR") == null) ? "" : mi.inData.get("PLGR").trim();
		Long MERE = mi.in.get("MERE");
		Integer OPNO = mi.in.get("OPNO");
		String MTNO = (mi.inData.get("MTNO") == null) ? "" : mi.inData.get("MTNO").trim();
		Double OLQT = mi.in.get("OLQT");
		Double NEQT = mi.in.get("NEQT");
		String WHST = (mi.inData.get("WHST") == null) ? "" : mi.inData.get("WHST").trim();
		Integer SPMT = mi.in.get("SPMT");
		
		if(!checkInputs(CONO, FACI, PLGR, MERE, OPNO, MTNO, OLQT, NEQT)) {
			return;
		}

			
		double reqt = NEQT - OLQT;
			
		DBAction extma2Record = database.table("EXTMA2").index("00").selection("EXREQT","EXCHNO").build();
		DBContainer extma2Container = extma2Record.createContainer();
		extma2Container.setInt("EXCONO", CONO);
		extma2Container.setString("EXFACI", FACI);
		extma2Container.setString("EXPLGR",PLGR);
		extma2Container.setLong("EXMERE", MERE);
		extma2Container.setInt("EXOPNO", OPNO);
		extma2Container.setString("EXMTNO", MTNO);

		if(WHST > "90")
			return;

		if(!extma2Record.readLock(extma2Container, { LockedResult updatedRecord ->
					updatedRecord.setDouble("EXREQT", reqt + updatedRecord.getDouble("EXREQT"));
					updateTrackingField(updatedRecord, "EX");
					updatedRecord.update();
				})){
			extma2Container.setDouble("EXREQT", NEQT);
			extma2Container.setInt("EXSPMT", SPMT);
			insertTrackingField(extma2Container, "EX");
			extma2Record.insert(extma2Container);
		}

		Map<String,String> parameters =  ["CONO":CONO.toString(),FACI:FACI,MERE:MERE.toString(),INDX:"10",PLGR:PLGR,OPNO:OPNO.toString()];
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
	 * @param mtno
	 * @param olqt
	 * @param neqt
	 * @return true if no error.
	 */
	private boolean checkInputs(Integer cono, String  faci, String plgr, Long mere, Integer opno, String mtno, Double olqt, Double neqt ) {
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
			mi.error("La note mère "+mere+" n'existe pas.");
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

		if(olqt == null) {
			mi.error("L'ancienne quantité réservée est obligatoire.");
			return false;
		}

		if(neqt == null) {
			mi.error("La nouvelle quantité réservée est obligatoire.");
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
