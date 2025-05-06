import java.time.Instant
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * README
 *
 * Name: EXT005MI.LstNOTES
 * Description: Liste les notes mères
 * Date                         Changed By                         Description
 * 20240216                     ddecosterd@hetic3.fr     	création
 * 20250409						ddecosterd@hetic3.fr		increase limit to 10000 of MWOOPE readall
 */
public class LstNOTES extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;
	private final MICallerAPI miCaller;

	private String cusPlgr = "E_CPDES";
	private String cusEmplMorceaux = "PEAUSSERIE";
	private String cusMorceau = "ZZ1";
	private int cusCseq = 7778;
	private int cusMseq = 8889;

	public LstNOTES(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility, MICallerAPI miCaller) {
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

		if(!checkInputs(CONO, FACI, PLGR))
			return;

		init(CONO, FACI, PLGR);

		fillTR1(CONO, FACI, PLGR);

		liste(CONO, FACI, PLGR);
	}

	/**
	 * Check input values
	 * @param cono
	 * @param faci
	 * @param plgr
	 * @return true if no error.
	 */
	private boolean checkInputs(Integer cono, String  faci, String plgr) {
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

		return true;
	}

	/**
	 * Get config values
	 * @param cono
	 * @param faci
	 * @param plgr
	 */
	private void init(int cono, String faci, String plgr) {
		DBAction CUGEX1Record = database.table("CUGEX1").index("00").selection("F1A030","F1A130","F1A230","F1N096","F1N196").build();
		DBContainer CUGEX1Container = CUGEX1Record.createContainer();

		CUGEX1Container.setInt("F1CONO", cono);
		CUGEX1Container.setString("F1FILE", "EXTEND");
		CUGEX1Container.setString("F1PK01", "IPRD002");
		if(CUGEX1Record.read(CUGEX1Container)) {
			cusPlgr = CUGEX1Container.getString("F1A030");
			cusEmplMorceaux = CUGEX1Container.getString("F1A130");
			cusMorceau = CUGEX1Container.getString("F1A230");
			cusCseq = CUGEX1Container.get("F1N096");
			cusMseq = CUGEX1Container.get("F1N196");
		}else {
			CUGEX1Container.setString("F1A030",cusPlgr);
			CUGEX1Container.setString("F1A130", cusEmplMorceaux);
			CUGEX1Container.setString("F1A230", cusMorceau);
			CUGEX1Container.set("F1N096", cusCseq);
			CUGEX1Container.set("F1N196", cusMseq);
			insertTrackingField(CUGEX1Container, "F1");
			CUGEX1Record.insert(CUGEX1Container);
		}

	}

	/**
	 * Copy a subset of MWOOPE and add information from MITMAS
	 * @param cono
	 * @param faci
	 * @param plgr
	 */
	private void fillTR1(int cono, String faci, String plgr) {
		ExpressionFactory mwoopeExpressionFactory = database.getExpressionFactory("MWOOPE");
		mwoopeExpressionFactory = mwoopeExpressionFactory.gt("VOSCHS", "0");

		DBAction mwoopeRecord = database.table("MWOOPE").index("95").matching(mwoopeExpressionFactory).selection("VOPRNO", "VOMFNO", "VOOPNO", "VOSTDT","VOSCHS", "VOSCHN", "VOWOST").build();
		DBContainer mwoopeContainer = mwoopeRecord.createContainer();
		mwoopeContainer.setInt("VOCONO", cono);
		mwoopeContainer.setString("VOFACI", faci);
		mwoopeContainer.setString("VOPLGR", plgr);

		mwoopeRecord.readAll(mwoopeContainer, 3, 10000, { DBContainer mwoopeData ->
			insertUpdateDelExttr1(cono, faci, plgr, mwoopeData);
		});

	}

	/**
	 * Insert or update EXTTR1 with data from MWOOPE
	 * @param cono
	 * @param faci
	 * @param plgr
	 * @param mwoopeData
	 */
	private void insertUpdateDelExttr1( int cono, String faci, String plgr, DBContainer mwoopeData) {
		DBAction exttr1Record = database.table("EXTTR1").index("00").build();
		DBContainer exttr1Container = exttr1Record.createContainer();

		DBAction mwohedRecord = database.table("MWOHED").index("00").selection("VHSCHN","VHWHST").build();
		DBContainer mwohedContainer = mwohedRecord.createContainer();

		mwohedContainer.setInt("VHCONO", cono);
		mwohedContainer.setString("VHFACI", faci);
		mwohedContainer.setString("VHPRNO", mwoopeData.getString("VOPRNO"));
		mwohedContainer.setString("VHMFNO", mwoopeData.getString("VOMFNO"));
		mwohedRecord.read(mwohedContainer);

		if(mwohedContainer.getString("VHWHST").equals("90") || mwohedContainer.getLong("VHSCHN") == 0 || mwoopeData.getString("VOWOST") == "90" ) {
			exttr1Container.setInt("EXCONO", cono);
			exttr1Container.setString("EXFACI", faci);
			exttr1Container.setString("EXPLGR", plgr);
			exttr1Container.setInt("EXOPNO", mwoopeData.getInt("VOOPNO"));
			exttr1Container.set("EXMERE", mwohedContainer.get("VHSCHN"));
			exttr1Container.setString("EXPRNO", mwoopeData.getString("VOPRNO"));
			exttr1Container.setString("EXMFNO", mwoopeData.getString("VOMFNO"));
			exttr1Record.readLock(exttr1Container, { LockedResult entry ->
				entry.delete();
			});
			return;
		}

		DBAction mitmasRecord = database.table("MITMAS").index("00").selection("MMGRP1","MMGRP2","MMGRP3","MMGRP4","MMGRP5","MMITDS").build();
		DBContainer mitmasContainer = mitmasRecord.createContainer();
		mitmasContainer.setInt("MMCONO", cono);
		mitmasContainer.setString("MMITNO", mwoopeData.getString("VOPRNO"));
		mitmasRecord.read(mitmasContainer);

		String tx40 ="";
		DBAction mitschRecord = database.table("MITSCH").index("00").selection("SGTX40").build();
		DBContainer mitschContainer = mitschRecord.createContainer();
		mitschContainer.setInt("SGCONO", cono);
		mitschContainer.setInt("SGGLVL", 3);
		mitschContainer.setString("SGSGP0", mitmasContainer.getString("MMGRP3"));
		if(mitschRecord.read(mitschContainer)) {
			tx40 = mitschContainer.getString("SGTX40");
		}

		StringBuilder tige = new StringBuilder(44);
		if(mitmasContainer.getString("MMGRP3").length() > 3) {
			tige.append(mitmasContainer.getString("MMGRP3").substring(0,3));
		}else {
			tige.append(mitmasContainer.getString("MMGRP3"))
		}
		if(mitmasContainer.getString("MMGRP3").length() > 0) {
			tige.append(" ");
		}
		tige.append(tx40);

		exttr1Container.setInt("EXCONO", cono);
		exttr1Container.setString("EXFACI", faci);
		exttr1Container.setString("EXPLGR", plgr);
		exttr1Container.setInt("EXOPNO", mwoopeData.getInt("VOOPNO"));
		exttr1Container.set("EXMERE", mwohedContainer.get("VHSCHN"));
		exttr1Container.setString("EXPRNO", mwoopeData.getString("VOPRNO"));
		exttr1Container.setString("EXMFNO", mwoopeData.getString("VOMFNO"));
		if(!exttr1Record.readLock(exttr1Container, { LockedResult updatedRecord ->
					updatedRecord.setLong("EXDEBI",  mwoopeData.getLong("VOSCHN"));
					updatedRecord.setString("EXGRP1", mitmasContainer.getString("MMGRP1"));
					updatedRecord.setString("EXGRP2", mitmasContainer.getString("MMGRP2"));
					updatedRecord.setString("EXGRP3", mitmasContainer.getString("MMGRP3"));
					updatedRecord.setString("EXGRP4", mitmasContainer.getString("MMGRP4"));
					updatedRecord.setString("EXGRP5", mitmasContainer.getString("MMGRP5"));
					updatedRecord.setString("EXITDS", mitmasContainer.getString("MMITDS"));
					updatedRecord.setString("EXTIGE", tige.toString());
					updatedRecord.setInt("EXSTDT", mwoopeData.getInt("VOSTDT"));
					updatedRecord.setLong("EXSCHS", mwoopeData.getLong("VOSCHS"));
					updatedRecord.setString("EXWOST", mwoopeData.getString("VOWOST"));
					updateTrackingField(updatedRecord, "EX");
					updatedRecord.update();
				})){
			exttr1Container.set("EXDEBI", mwoopeData.get("VOSCHN"));
			exttr1Container.setString("EXGRP1", mitmasContainer.getString("MMGRP1"));
			exttr1Container.setString("EXGRP2", mitmasContainer.getString("MMGRP2"));
			exttr1Container.setString("EXGRP3", mitmasContainer.getString("MMGRP3"));
			exttr1Container.setString("EXGRP4", mitmasContainer.getString("MMGRP4"));
			exttr1Container.setString("EXGRP5", mitmasContainer.getString("MMGRP5"));
			exttr1Container.setString("EXITDS", mitmasContainer.getString("MMITDS"));
			exttr1Container.setString("EXTIGE", tige.toString());
			exttr1Container.setInt("EXSTDT", mwoopeData.getInt("VOSTDT"));
			exttr1Container.setLong("EXSCHS", mwoopeData.getLong("VOSCHS"));
			exttr1Container.setString("EXWOST", mwoopeData.getString("VOWOST"));
			insertTrackingField(exttr1Container, "EX");
			exttr1Record.insert(exttr1Container);
		}

	}

	/**
	 * build the list result
	 * @param cono
	 * @param faci
	 * @param plgr
	 * @return
	 */
	private liste(int cono, String faci, String plgr) {
		DBAction exttr1Record = database.table("EXTTR1").index("00").selection("EXWOST","EXSCHS","EXGRP1","EXGRP2","EXGRP3","EXGRP4","EXGRP5","EXTIGE","EXSTDT","EXITDS").build();
		DBContainer exttr1Container = exttr1Record.createContainer();
		exttr1Container.setInt("EXCONO", cono);
		exttr1Container.setString("EXFACI", faci);
		exttr1Container.setString("EXPLGR", plgr);

		long mere = 0;
		long dess = 0;

		int wwopno = 0;
		long wwmere = 0;
		long schs = 0;
		boolean firsLine = true;
		Double nbof = 0;
		int nbRead = exttr1Record.readAll(exttr1Container, 3, 1000, { DBContainer exttr1Data ->

			if(firsLine) {
				wwopno = exttr1Data.getInt("EXOPNO");
				wwmere = exttr1Data.getLong("EXMERE");
				schs =  exttr1Data.getLong("EXSCHS");
				firsLine = false;
			}

			if(mere != exttr1Data.getLong("EXMERE")) {
				dess = 0;

				DBAction mpdwctRecord = database.table("MPDWCT").selection("PPKIWG").build();
				DBContainer mpdwctContainer = mpdwctRecord.createContainer();
				mpdwctContainer.setInt("PPCONO", exttr1Data.getInt("EXCONO"));
				mpdwctContainer.setString("PPFACI", exttr1Data.getString("EXFACI"));
				mpdwctContainer.setString("PPPLGR", cusPlgr);
				if(mpdwctRecord.read(mpdwctContainer)) {
					DBAction mwoopeRecord = database.table("MWOOPE").index("00").selection("VOSCHN").build();
					DBContainer mwoopeContainer = mwoopeRecord.createContainer();
					mwoopeContainer.setInt("VOCONO", exttr1Data.getInt("EXCONO"));
					mwoopeContainer.setString("VOFACI", exttr1Data.getString("EXFACI"));
					mwoopeContainer.setString("VOPRNO", exttr1Data.getString("EXPRNO"));
					mwoopeContainer.setString("VOMFNO", exttr1Data.getString("EXMFNO"));
					mwoopeContainer.setInt("VOOPNO", mpdwctContainer.getInt("PPKIWG"));

					if(mwoopeRecord.read(mwoopeContainer)) {
						dess = mwoopeContainer.getLong("VOSCHN");
					}
				}
				mere = exttr1Data.getLong("EXMERE");
			}

			if(exttr1Data.getLong("EXMERE") != wwmere || exttr1Data.getInt("EXOPNO") != wwopno) {
				mi.write();
				wwopno = exttr1Data.getInt("EXOPNO");
				wwmere = exttr1Data.getLong("EXMERE");
				schs =  exttr1Data.getLong("EXSCHS");
				nbof = 0;
			}
			DBAction mwohedRecord = database.table("MWOHED").selection("VHORQT").build();
			DBContainer mwohedContainer = mwohedRecord.createContainer();
			mwohedContainer.setInt("VHCONO", cono);
			mwohedContainer.setString("VHFACI", faci);
			mwohedContainer.setString("VHPRNO", exttr1Data.getString("EXPRNO"));
			mwohedContainer.setString("VHMFNO", exttr1Data.getString("EXMFNO"));
			mwohedRecord.read(mwohedContainer);
			nbof += mwohedContainer.getDouble("VHORQT");
			mi.getOutData().put("OPNO", exttr1Data.get("EXOPNO").toString());
			mi.getOutData().put("MERE", exttr1Data.get("EXMERE").toString());
			if(exttr1Data.getLong("EXDEBI")!= exttr1Data.getLong("EXMERE"))
				mi.getOutData().put("DEBI", exttr1Data.get("EXDEBI").toString());
			else
				mi.getOutData().put("DEBI", "0");
			mi.getOutData().put("GRP1", exttr1Data.getString("EXGRP1"));
			mi.getOutData().put("GRP2", exttr1Data.getString("EXGRP2"));
			mi.getOutData().put("GRP3", exttr1Data.getString("EXGRP3"));
			mi.getOutData().put("GRP4", exttr1Data.getString("EXGRP4"));
			mi.getOutData().put("GRP5", exttr1Data.getString("EXGRP5"));
			mi.getOutData().put("NBOF", Math.round(nbof).toString());
			mi.getOutData().put("TIGE", exttr1Data.getString("EXTIGE"));
			mi.getOutData().put("STDT", exttr1Data.get("EXSTDT").toString());
			mi.getOutData().put("DESS", dess.toString());
			mi.getOutData().put("STYL", exttr1Data.getString("EXGRP1").trim()+" "+exttr1Data.getString("EXGRP2").trim()+" "+exttr1Data.getString("EXGRP3").trim()+" "+exttr1Data.getString("EXGRP4").trim());
			mi.getOutData().put("QDEB", "0");
			mi.getOutData().put("ITDS", exttr1Data.getString("EXITDS"));
			mi.getOutData().put("SCHS", schs.toString());
			mi.getOutData().put("WOST", exttr1Data.getString("EXWOST"));
		} );
		if(nbRead > 0) {
			mi.write();
		}
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
