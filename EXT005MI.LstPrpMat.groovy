/**
 * README
 *
 * Name: EXT005MI.LstPrpMat
 * Description: Liste les besoins composants
 * Date                         Changed By                         Description
 * 20240221                     ddecosterd@hetic3.fr     	création
 */
public class LstPrpMat extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;
	private final MICallerAPI miCaller;

	private String cusMorceau;

	public LstPrpMat(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility, MICallerAPI miCaller) {
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
		Long DEBI = mi.in.get("DEBI");
		Integer MRCX = mi.in.get("MRCX");

		if(!checkInputs(CONO, FACI, PLGR, DEBI, MRCX))
			return;

		String BJNO = createBJNO();
		if(BJNO == null)
			return;

		init(CONO);

		fillTR2(BJNO, CONO, FACI, PLGR, DEBI, MRCX);

		liste(BJNO, CONO, MRCX);

		clearTables(BJNO);
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
		updatedRecord.set(prefix+"LMDT", (Integer) utility.call("DateUtil", "currentDateY8AsInt"));
		updatedRecord.set(prefix+"CHID", program.getUser());
		updatedRecord.setInt(prefix+"CHNO", CHNO);
	}

	/**
	 * Get config values
	 * @param cono
	 */
	private init(int cono) {
		DBAction CUGEX1Record = database.table("CUGEX1").index("00").selection("F1A030","F1N096","F1N196","F1CHB1").build();
		DBContainer CUGEX1Container = CUGEX1Record.createContainer();
		CUGEX1Container.setInt("F1CONO", cono);
		CUGEX1Container.setString("F1FILE", "EXTEND");
		CUGEX1Container.setString("F1PK01", "IPRD002");
		if(CUGEX1Record.read(CUGEX1Container)) {
			cusMorceau = CUGEX1Container.getString("F1A230");
		}

	}

	/**
	 * Check input values
	 * @param cono
	 * @param faci
	 * @param plgr
	 * @param debi
	 * @param mrcx
	 * @return true if no error.
	 */
	private boolean checkInputs(Integer cono, String  faci, String plgr, Long debi, Integer mrcx) {
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

		if(debi == null) {
			mi.error("La note de débit est obligatoire.");
			return false;
		}
		if(!utility.call("CheckUtil", "checkSCHNExist", database, cono, debi)) {
			mi.error("La note de débit est inexistante");
			return false;
		}

		if(mrcx == null) {
			mi.error("Le paramètre MRCX est obligatoire.");
			return false;
		}
		if( mrcx != 0 && mrcx != 1) {
			mi.error("Le paramètre MRCX n'accepte que 0 et 1 comme valeur.");
			return false;
		}

		return true;
	}

	/**
	 * Get nex job number
	 * @return new job number
	 */
	private String createBJNO() {
		String bjno = null;
		miCaller.call("CRS165MI", "RtvNextNumber", [NBTY:"Z1",NBID:"1"], { Map<String, String> response ->
			if(response.containsKey("error")) {
				mi.error(response.errorMessage);
				return;
			}else {
				bjno = response.NBNR;
			}
		});
		int cono = (Integer) program.getLDAZD().CONO;
		return cono.toString()+" "+bjno;
	}

	/**
	 * emptying table EXTTR2
	 * @param bjno
	 */
	private void clearTables(String bjno) {
		//emptying EXTTR2
		DBAction tr2Record = database.table("EXTTR2").index("00").build();
		DBContainer tr2Container = tr2Record.createContainer();
		tr2Container.setString("EXBJNO", bjno);
		tr2Record.readAll(tr2Container, 1, 1000,{ DBContainer tr2Data ->
			DBAction supTr2Record = database.table("EXTTR2").index("00").build();
			DBContainer supTr2Container = supTr2Record.createContainer();
			supTr2Container.setString("EXBJNO", bjno);
			supTr2Container.setInt("EXCONO", tr2Data.getInt("EXCONO"));
			supTr2Container.setString("EXFACI", tr2Data.getString("EXFACI"));
			supTr2Container.setString("EXPLGR", tr2Data.getString("EXPLGR"));
			supTr2Container.setLong("EXMERE", tr2Data.getLong("EXMERE"));
			supTr2Container.setInt("EXOPNO", tr2Data.getInt("EXOPNO"));
			supTr2Container.setString("EXMTNO", tr2Data.getString("EXMTNO"));
			supTr2Record.readLock(tr2Container, {  LockedResult entry ->
				entry.delete();
			});
		});
	}

	/**
	 * Fill EXTTR2 wuth data from MWOMAT 
	 * @param bjno
	 * @param cono
	 * @param faci
	 * @param plgr
	 * @param debi
	 * @param mrcx
	 */
	private void fillTR2(String bjno, int cono, String faci, String plgr, Long debi, int mrcx) {
		DBAction mwoopeRecord = database.table("MWOOPE").index("70").selection("VOPRNO", "VOMFNO", "VOOPNO").build();
		DBContainer mwoopeContainer = mwoopeRecord.createContainer();
		mwoopeContainer.setInt("VOCONO", cono);
		mwoopeContainer.setString("VOFACI", faci);
		mwoopeContainer.setString("VOPLGR", plgr);
		mwoopeContainer.setLong("VOSCHN", debi);

		mwoopeRecord.readAll(mwoopeContainer, 4, 1000, { DBContainer mwoopeData ->
			DBAction mwohedRecord = database.table("MWOHED").index("00").selection("VHSCHN").build();
			DBContainer mwohedContainer = mwohedRecord.createContainer();
			mwohedContainer.setInt("VHCONO", cono);
			mwohedContainer.setString("VHFACI", faci);
			mwohedContainer.setString("VHPRNO", mwoopeData.getString("VOPRNO"));
			mwohedContainer.setString("VHMFNO", mwoopeData.getString("VOMFNO"));
			mwohedRecord.read(mwohedContainer);

			DBAction mwomatRecord = database.table("MWOMAT").index("10").selection("VMMTNO", "VMREQT", "VMRPQT", "VMBYPR").build();
			DBContainer mwomatContainer = mwomatRecord.createContainer();
			mwomatContainer.setInt("VMCONO", cono);
			mwomatContainer.setString("VMFACI", faci);
			mwomatContainer.setString("VMPRNO", mwoopeData.getString("VOPRNO"));
			mwomatContainer.setString("VMMFNO", mwoopeData.getString("VOMFNO"));
			mwomatContainer.setInt("VMOPNO", mwoopeData.getInt("VOOPNO"));

			mwomatRecord.readAll(mwomatContainer, 5, 1000, { DBContainer mwomatData ->
				DBAction mitmasRecord = database.table("MITMAS").index("00").selection("MMFUDS").build();
				DBContainer mitmasContainer = mitmasRecord.createContainer();
				mitmasContainer.setInt("MMCONO", cono);
				mitmasContainer.setString("MMITNO", mwomatData.getString("VMMTNO"));
				mitmasRecord.read(mitmasContainer);

				if( mwomatData.getInt("VMBYPR") == 1 && mitmasContainer.getString("MMGRTI").equals(cusMorceau) ||  mrcx == 0) {
					DBAction exttr2Record = database.table("EXTTR2").index("00").selection("EXREQT", "EXRPQT").build();
					DBContainer exttr2Container = exttr2Record.createContainer();
					exttr2Container.setString("EXBJNO", bjno);
					exttr2Container.setInt("EXCONO", cono);
					exttr2Container.setString("EXFACI", faci);
					exttr2Container.set("EXMERE", mwohedContainer.get("VHSCHN"));
					exttr2Container.setInt("EXOPNO",mwoopeData.getInt("VOOPNO"));
					exttr2Container.setString("EXMTNO", mwomatData.getString("VMMTNO"));

					if(!exttr2Record.readLock(exttr2Container,{LockedResult updatedRecord ->
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
				}
			});
		});
	}

	/**
	 * build the list
	 * @param bjno
	 * @param cono
	 * @param mrcx
	 */
	private void liste(String bjno, int cono, int mrcx) {
		ExpressionFactory exttr2PExpressionFactory = database.getExpressionFactory("EXTTR2");
		exttr2PExpressionFactory =  exttr2PExpressionFactory.ne("EXREQT","0");
		DBAction exttr2Record = database.table("EXTTR2").index("00").matching(exttr2PExpressionFactory).selection("EXMERE", "EXOPNO", "EXMTNO", "EXRPQT", "EXREQT").build();
		DBContainer exttr2Container = exttr2Record.createContainer();
		exttr2Container.setString("EXBJNO", bjno);
		double rpqt = 0.0;
		double reqt = 0.0;
		Long mere = 0;
		int opno = 0;
		String mtno = "";
		String fuds = "";

		int nbRecords = exttr2Record.readAll(exttr2Container, 1 , 1000, { DBContainer exttr2Data ->
			DBAction mitmasRecord = database.table("MITMAS").index("00").selection("MMFUDS").build();
			DBContainer mitmasContainer = mitmasRecord.createContainer();
			mitmasContainer.setInt("MMCONO", cono);
			mitmasContainer.setString("MMITNO", exttr2Data.getString("EXMTNO"));
			mitmasRecord.read(mitmasContainer);
			fuds = mitmasContainer.getString("MMFUDS");

			if(mrcx == 0) {
				writeOutput(exttr2Data.getInt("EXOPNO"), exttr2Data.getLong("EXMERE"), exttr2Data.getString("EXMTNO"), fuds, 
							exttr2Data.getDouble("EXRPQT"), exttr2Data.getDouble("EXREQT"));
			}else {
				if(mere != 0 && mere != exttr2Data.getLong("EXMERE") || opno != 0 && opno != exttr2Data.getInt("EXOPNO"))
				{
					writeOutput(opno,mere, mtno, fuds, rpqt, reqt);
					rpqt = 0.0;
					reqt = 0.0;
					
				}
				rpqt += exttr2Data.getDouble("EXRPQT");
				reqt += exttr2Data.getDouble("EXREQT");
				opno = exttr2Data.getInt("EXOPNO");
				mere = exttr2Data.getLong("EXMERE");
				mtno = exttr2Data.getString("EXMTNO");
			}
		});
	
		if(mrcx == 1 && nbRecords> 0) {
			writeOutput(opno,mere, mtno, fuds, rpqt, reqt);
		}

	}

	/**
	 * Write a line of the MI
	 * @param opno
	 * @param mere
	 * @param mtno
	 * @param fuds
	 * @param rpqt
	 * @param reqt
	 */
	private void writeOutput(double opno, long mere, String mtno, String fuds, double rpqt, double reqt ) {
		mi.getOutData().put("OPNO", opno.toString());
		mi.getOutData().put("MERE", mere.toString());
		mi.getOutData().put("MTNO", mtno);
		mi.getOutData().put("FUDS", fuds);
		mi.getOutData().put("RPQT", rpqt.toString());
		mi.getOutData().put("REQT", reqt.toString());
		mi.write();
	}
}