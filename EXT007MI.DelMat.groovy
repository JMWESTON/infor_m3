/**
 * README
 *
 * Name: EXT007MI.DelMat
 * Description: Supprime le matériau dans la table exmat2
 * Date                         Changed By                    Description
 * 20250130                     ddecosterd@hetic3.fr     		création
 * 20250221						ddecosterd@hetic3.fr			fix CHNO not updated. Search MERE in MWOHED instead of getting the call to remove error in CMS041. Delete material only when there is'nt one in mwomat for the given OPNO
 */
public class DelMat extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;
	private final MICallerAPI miCaller;

	public DelMat(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility, MICallerAPI miCaller) {
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
		Integer OPNO = mi.in.get("OPNO");
		String MTNO = (mi.inData.get("MTNO") == null) ? "" : mi.inData.get("MTNO").trim();
		Double REQT = mi.in.get("REQT");
		
		if(!checkInputs(CONO, FACI, PRNO, MFNO, PLGR, OPNO, MTNO)) {
			return;
		}

		DBAction mwohedRecord = database.table("MWOHED").index("00").selection("VHSCHN",'VHWHST').build();
		DBContainer mwohedContainer = mwohedRecord.createContainer();
		mwohedContainer.setInt("VHCONO", CONO);
		mwohedContainer.setString("VHFACI", FACI);
		mwohedContainer.setString("VHPRNO", PRNO);
		mwohedContainer.setString("VHMFNO", MFNO);
		if(!mwohedRecord.read(mwohedContainer))
			return;



		DBAction exmat2Record = database.table("EXTMA2").index("00").selection("EXREQT","EXCHNO").build();
		DBContainer exmat2Container = exmat2Record.createContainer();
		exmat2Container.setInt("EXCONO", CONO);
		exmat2Container.setString("EXFACI", FACI);
		exmat2Container.setString("EXPLGR",PLGR);
		exmat2Container.setLong("EXMERE", mwohedContainer.getLong("VHSCHN"));
		exmat2Container.setInt("EXOPNO", OPNO);
		exmat2Container.setString("EXMTNO", MTNO);

		if(mwohedContainer.getString("VHWHST") > "90")
			return;

		boolean found = exmat2Record.readLock(exmat2Container, { LockedResult updatedRecord ->
			double newQty = updatedRecord.getDouble("EXREQT") - REQT;
			ExpressionFactory mwomatExpressionFactory = database.getExpressionFactory("MWOMAT");
			mwomatExpressionFactory = mwomatExpressionFactory.eq("VMMTNO", MTNO);
			DBAction mwomatRecord = database.table("MWOMAT").index("10").matching(mwomatExpressionFactory).build();
			DBContainer mwomatContainer = mwomatRecord.createContainer();
			mwomatContainer.setInt("VMCONO", CONO);
			mwomatContainer.setString("VMFACI",FACI);
			mwomatContainer.setString("VMPRNO", PRNO);
			mwomatContainer.setString("VMMFNO", MFNO);
			mwomatContainer.setInt("VMOPNO", OPNO);
			int read  = mwomatRecord.readAll(mwomatContainer, 5, 1, {});

			if( read == 0 ) {
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

		Map<String,String> parameters =  ["CONO":CONO.toString(),FACI:FACI,MERE:mwohedContainer.getLong("VHSCHN").toString(),INDX:"10",PLGR:PLGR,OPNO:OPNO.toString(),NDEL:"1"];
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
	 * @param prno
	 * @param mfno
	 * @param opno
	 * @param mtno
	 * @return true if no error.
	 */
	private boolean checkInputs(Integer cono, String  faci, String prno, String mfno, String plgr, Integer opno, String mtno ) {
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
			mi.error("Le poste de charge "+plgr+" est inexistant");
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

		if(mtno == null || mtno.isEmpty()) {
			mi.error("Numéro de composant est obligatoire.");
			return false;
		}

		return true;
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
