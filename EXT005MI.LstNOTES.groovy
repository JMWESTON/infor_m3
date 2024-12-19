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
 */
public class LstNOTES extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;
	private final MICallerAPI miCaller;

	private String CUS_PLGR = "E_CPDES";
	private String CUS_EMPL_MORCEAUX = "PEAUSSERIE";
	private String CUS_MORCEAU = "ZZ1";
	private int CUS_CSEQ = 7778;
	private int CUS_MSEQ = 8889;
	private long LAST_UPDATE = 0;
	private String LAST_CALL_EXT001 = "";
	private String TABLE_MATERIAL = "EXT002";

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
		Integer SCHS = mi.in.get("SCHS");

		if(!checkInputs(CONO, FACI, PLGR))
			return;

		init(CONO, FACI, PLGR);

		fillTR1(CONO, FACI, PLGR);

		boolean shchFilter = SCHS != null && SCHS == 1;

		liste(CONO, FACI, PLGR, LAST_CALL_EXT001, TABLE_MATERIAL, shchFilter);
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
		if(!this.utility.call("CheckUtil", "checkConoExist", database, cono)) {
			mi.error("La division est inexistante.");
			return false;
		}

		if(faci.isEmpty()) {
			mi.error("L'établissement est obligatoire.");
			return false;
		}
		if(!this.utility.call("CheckUtil", "checkFacilityExist", database, cono, faci)) {
			mi.error("L'établissement est inexistant.");
			return false;
		}

		if(plgr == null) {
			mi.error("Le poste de charge est obligatoire.");
			return false;
		}
		if(!this.utility.call("CheckUtil", "checkPLGRExist", database, cono, faci, plgr)) {
			mi.error("Le poste de charge est inexistant");
			return false;
		}

		return true;
	}

	/**
	 * Get default values from CUGEX1
	 * @param cono
	 * @param faci
	 * @param plgr
	 */
	private void init(int cono, String faci, String plgr) {
		DBAction CUGEX1Record = database.table("CUGEX1").index("00").selection("F1A030","F1A130","F1A230","F1N096","F1N196").build();
		DBContainer CUGEX1Container = CUGEX1Record.createContainer();
		CUGEX1Container.setInt("F1CONO", cono);
		CUGEX1Container.setString("F1FILE", "BATCH");
		CUGEX1Container.setString("F1PK01", "EXT001");

		if(CUGEX1Record.read(CUGEX1Container)) {
			LAST_CALL_EXT001 = CUGEX1Container.getString("F1A030");
			TABLE_MATERIAL = CUGEX1Container.getString("F1A130");
		}

		CUGEX1Container.setString("F1FILE", "EXTEND");
		CUGEX1Container.setString("F1PK01", "IPRD002");
		if(CUGEX1Record.read(CUGEX1Container)) {
			CUS_PLGR = CUGEX1Container.getString("F1A030");
			CUS_EMPL_MORCEAUX = CUGEX1Container.getString("F1A130");
			CUS_MORCEAU = CUGEX1Container.getString("F1A230");
			CUS_CSEQ = CUGEX1Container.get("F1N096");
			CUS_MSEQ = CUGEX1Container.get("F1N196");
		}else {
			CUGEX1Container.setString("F1A030",CUS_PLGR);
			CUGEX1Container.setString("F1A130", CUS_EMPL_MORCEAUX);
			CUGEX1Container.setString("F1A230", CUS_MORCEAU);
			CUGEX1Container.set("F1N096", CUS_CSEQ);
			CUGEX1Container.set("F1N196", CUS_MSEQ);
			insertTrackingField(CUGEX1Container, "F1");
			CUGEX1Record.insert(CUGEX1Container);
		}

		CUGEX1Container.setString("F1PK02", faci);
		CUGEX1Container.setString("F1PK03", plgr);
		if(CUGEX1Record.read(CUGEX1Container)) {
			LAST_UPDATE = CUGEX1Container.getDouble("F1N096").toLong()-400000;
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
		mwoopeExpressionFactory = mwoopeExpressionFactory.gt("VOLMTS", LAST_UPDATE.toString());

		DBAction mwoopeRecord = database.table("MWOOPE").index("95").matching(mwoopeExpressionFactory).selection("VOPRNO", "VOMFNO", "VOOPNO", "VOSTDT","VOSCHS", "VOSCHN", "VOWOST").build();
		DBContainer mwoopeContainer = mwoopeRecord.createContainer();
		mwoopeContainer.setInt("VOCONO", cono);
		mwoopeContainer.setString("VOFACI", faci);
		mwoopeContainer.setString("VOPLGR", plgr);

		Set<Long> schnSet = new HashSet<Long>();
		long now = Instant.now().toEpochMilli();

		mwoopeRecord.readAll(mwoopeContainer, 3, { DBContainer mwoopeData ->
			insertUpdateDelExttr1(cono, faci, plgr, mwoopeData);
		});

		if(LAST_UPDATE>0)
		{
			//Update EXTTR1 row who contains Items who have been modified since the last update.
			ExpressionFactory mitmasExpressionFactory = database.getExpressionFactory("MITMAS");
			mitmasExpressionFactory = mitmasExpressionFactory.gt("MMLMTS", LAST_UPDATE.toString());
			DBAction mitmasRecord = database.table("MITMAS").index("00").matching(mitmasExpressionFactory).build();
			DBContainer mitmasContainer = mitmasRecord.createContainer();
			mitmasContainer.setInt("MMCONO", cono);
			mitmasRecord.readAll(mitmasContainer,1,{DBContainer mitmasData ->
				mwoopeExpressionFactory = database.getExpressionFactory("MWOOPE");
				mwoopeExpressionFactory = mwoopeExpressionFactory.le("VOLMTS", LAST_UPDATE.toString()).and(mwoopeExpressionFactory.eq("VOPRNO", mitmasData.getString("MMITNO")));
				mwoopeRecord = database.table("MWOOPE").index("95").matching(mwoopeExpressionFactory).selection("VOPRNO", "VOMFNO", "VOOPNO", "VOSTDT","VOSCHS", "VOSCHN", "VOWOST").build();
				mwoopeContainer = mwoopeRecord.createContainer();
				mwoopeContainer.setInt("VOCONO", cono);
				mwoopeContainer.setString("VOFACI", faci);
				mwoopeContainer.setString("VOPLGR", plgr);
				mwoopeRecord.readAll(mwoopeContainer, 3, { DBContainer mwoopeData ->
					insertUpdateDelExttr1(cono, faci, plgr, mwoopeData);
				});
			});

			//Update EXTTR1 row who coonected to row from MWHOED who have been modified since the last update.
			ExpressionFactory mwohedExpressionFactory = database.getExpressionFactory("MWOHED");
			mwohedExpressionFactory = mwohedExpressionFactory.gt("VHLMTS", LAST_UPDATE.toString()).and(mwohedExpressionFactory.eq("VHPLGR", plgr));
			DBAction mwohedRecord = database.table("MWOHED").index("00").matching(mwohedExpressionFactory).build();
			DBContainer mwohedContainer = mwohedRecord.createContainer();
			mwohedContainer.setInt("VHCONO", cono);
			mwohedContainer.setString("VHFACI", faci);
			mwohedRecord.readAll(mwohedContainer, 2,{  DBContainer mwohedData ->
				mwoopeRecord = database.table("MWOOPE").index("00").selection("VOPRNO", "VOMFNO", "VOOPNO", "VOSTDT","VOSCHS", "VOSCHN", "VOWOST").build();
				mwoopeContainer = mwoopeRecord.createContainer();
				mwoopeContainer.setInt("VOCONO", cono);
				mwoopeContainer.setString("VOFACI", faci);
				mwoopeContainer.setString("VOPRNO", mwohedData.getString("VHPRNO"));
				mwoopeContainer.setString("VOMFNO", mwohedData.getString("VHMFNO"));
				mwoopeRecord.readAll(mwoopeContainer, 4, { DBContainer mwoopeData ->
					insertUpdateDelExttr1(cono, faci, plgr, mwoopeData);
				});
			});

		}

		saveLastUpdate(cono, faci, plgr, now);
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

		if(mwohedContainer.getString("VHWHST").equals("90") || mwohedContainer.getLong("VHSCHN") == 0 ) {
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
	 * Save in CUGEX1 the date of the last update for plgr
	 * @param cono
	 * @param faci
	 * @param plgr
	 * @param lastUpdate
	 */
	private void saveLastUpdate(int cono, String faci, String plgr, long lastUpdate) {
		DBAction CUGEX1Record = database.table("CUGEX1").index("00").selection("F1N096").build();
		DBContainer CUGEX1Container = CUGEX1Record.createContainer();
		CUGEX1Container.setInt("F1CONO", cono);
		CUGEX1Container.setString("F1FILE", "EXTEND");
		CUGEX1Container.setString("F1PK01", "IPRD002");
		CUGEX1Container.setString("F1PK02", faci);
		CUGEX1Container.setString("F1PK03", plgr);
		if(!CUGEX1Record.readLock(CUGEX1Container,{LockedResult updatedRecord ->
					updatedRecord.set("F1N096",  lastUpdate);
					updateTrackingField(updatedRecord, "F1");
					updatedRecord.update();
				})) {
			CUGEX1Container.set("F1N096",  lastUpdate);;
			insertTrackingField(CUGEX1Container, "F1");
			CUGEX1Record.insert(CUGEX1Container);
		}

	}

	/**
	 * build the list result
	 * @param cono
	 * @param faci
	 * @param plgr
	 * @param lastUpd the date of the last execution of EXT001
	 * @param sourceTable the table between EXT002 and EXT003 the most up-to-date
	 * @param schsFilter if we want to filter SCHS equal to zero
	 * @return
	 */
	private liste(int cono, String faci, String plgr, String lastUpd, String sourceTable, boolean schsFilter) {
		ExpressionFactory exttr1ExpressionFactory = database.getExpressionFactory("EXTTR1");
		exttr1ExpressionFactory = exttr1ExpressionFactory.ne("EXSCHS", "0");
		DBAction exttr1Record;
		if(schsFilter)
			exttr1Record = database.table("EXTTR1").index("00").matching(exttr1ExpressionFactory).selectAllFields().build();
		else
			exttr1Record = database.table("EXTTR1").index("00").selectAllFields().build();
		DBContainer exttr1Container = exttr1Record.createContainer();
		exttr1Container.setInt("EXCONO", cono);
		exttr1Container.setString("EXFACI", faci);
		exttr1Container.setString("EXPLGR", plgr);

		long mere = 0;
		String mtn1 = "";
		double req1 = 0;
		double rpq1 = 0;
		String mtn2 = "";
		double req2 = 0;
		double rpq2 = 0;
		long dess = 0;
		int nmat = 0;

		int wwopno = 0;
		long wwmere = 0;
		long schs = 0;
		/*exttr1Record.readAll(exttr1Container, 3, 1, {  DBContainer exttr1Data ->
		 wwopno = exttr1Data.getInt("EXOPNO");
		 wwmere = exttr1Data.getLong("EXMERE");
		 schs =  exttr1Data.getLong("EXSCHS");
		 });*/
		boolean firsLine = true;
		List<String> list = ["E_COUPBR","E_CPDES","E_CPDOU","E_CPFOU","E_MAROCP"];
		Double nbof = 0;
		int nbRead = exttr1Record.readAll(exttr1Container, 3, { DBContainer exttr1Data ->

			if(exttr1Data.getString("EXWOST").equals("90")) {
				if(schsFilter)
					return;

				if(!list.contains(plgr))
					return;
					
				ExpressionFactory mwoopeExpressionFactory = database.getExpressionFactory("MWOOPE");
				mwoopeExpressionFactory = mwoopeExpressionFactory.eq("VOPRNO", exttr1Data.getString("EXPRNO")).and(mwoopeExpressionFactory.eq("VOMFNO", exttr1Data.getString("EXMFNO")));
				DBAction mwoopeRecord = database.table("MWOOPE").index("70").matching(mwoopeExpressionFactory).build();
				DBContainer mwoopeContainer = mwoopeRecord.createContainer();
				mwoopeContainer.setInt("VOCONO", exttr1Data.getInt("EXCONO"));
				mwoopeContainer.setString("VOFACI", exttr1Data.getString("EXFACI"));
				mwoopeContainer.setString("VOPLGR", "S"+plgr.substring(1));
				boolean s_bipped = true;
				mwoopeRecord.readAll(mwoopeContainer, 3, 1, { DBContainer mwoopeData ->
					s_bipped = false;
				});
				if(s_bipped)
					return;
			}

			if(firsLine) {
				wwopno = exttr1Data.getInt("EXOPNO");
				wwmere = exttr1Data.getLong("EXMERE");
				schs =  exttr1Data.getLong("EXSCHS");
				firsLine = false;
			}

			if(mere != exttr1Data.getLong("EXMERE")) {
				mtn1 = "";
				req1 = 0;
				rpq1 = 0;
				mtn2 = "";
				req2 = 0;
				rpq2 = 0;
				dess = 0;
				nmat = 0;
				DBAction exttr2Record = database.table(sourceTable).index("10").selection("EXMTNO", "EXREQT", "EXRPQT").build();
				DBContainer exttr2Container = exttr2Record.createContainer();
				exttr2Container.setString("EXBJNO", "MATERIAL_CALC");
				exttr2Container.setInt("EXCONO", exttr1Data.getInt("EXCONO"));
				exttr2Container.setString("EXFACI", exttr1Data.getString("EXFACI"));
				exttr2Container.setString("EXPLGR", plgr);
				exttr2Container.set("EXMERE", exttr1Data.get("EXMERE"));
				exttr2Container.setInt("EXOPNO", exttr1Data.getInt("EXOPNO"));

				exttr2Record.readAll(exttr2Container, 5, { DBContainer exttr2Data ->
					if(nmat == 0 || exttr2Data.getDouble("EXREQT") != 0) {
						nmat ++;
						if(nmat == 1) {
							mtn1 = exttr2Data.getString("EXMTNO");
							req1 = exttr2Data.getDouble("EXREQT");
							rpq1 = exttr2Data.getDouble("EXRPQT");
						}
						if(nmat == 2) {
							mtn2 = exttr2Data.getString("EXMTNO");
							req2 = exttr2Data.getDouble("EXREQT");
							rpq2 = exttr2Data.getDouble("EXRPQT");
						}
					}
				});

				DBAction mpdwctRecord = database.table("MPDWCT").selection("PPKIWG").build();
				DBContainer mpdwctContainer = mpdwctRecord.createContainer();
				mpdwctContainer.setInt("PPCONO", exttr1Data.getInt("EXCONO"));
				mpdwctContainer.setString("PPFACI", exttr1Data.getString("EXFACI"));
				mpdwctContainer.setString("PPPLGR", CUS_PLGR);
				if(mpdwctRecord.read(mpdwctContainer)) {
					DBAction mwoopeRecord = database.table("MWOOPE").index("00").selection("VOSCHN").build();
					DBContainer mwoopeContainer = mwoopeRecord.createContainer();
					mwoopeContainer.setInt("VOCONO", exttr1Data.getInt("EXCONO"));
					mwoopeContainer.setString("VOFACI", exttr1Data.getString("EXFACI"));
					mwoopeContainer.setString("VOPRNO", exttr1Data.getString("EXPRNO"));
					mwoopeContainer.setString("VOMFNO", exttr1Data.getString("EXMFNO"));
					mwoopeContainer.setInt("VOOPNO", mpdwctContainer.getInt("PPKIWG"));

					if(mwoopeRecord.read(mwoopeContainer)) {
						dess = mwoopeContainer.getLong("VOSCHN"); //TODO recherche où
					}
				}
				mere = exttr1Data.getLong("EXMERE");
			}

			if(exttr1Data.getLong("EXMERE") != wwmere || exttr1Data.getInt("EXOPNO") != wwopno) {
				this.mi.write();
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
			this.mi.getOutData().put("OPNO", exttr1Data.get("EXOPNO").toString());
			this.mi.getOutData().put("MERE", exttr1Data.get("EXMERE").toString());
			if(exttr1Data.getLong("EXDEBI")!= exttr1Data.getLong("EXMERE"))
				this.mi.getOutData().put("DEBI", exttr1Data.get("EXDEBI").toString());
			else
				this.mi.getOutData().put("DEBI", "0");
			this.mi.getOutData().put("GRP1", exttr1Data.getString("EXGRP1"));
			this.mi.getOutData().put("GRP2", exttr1Data.getString("EXGRP2"));
			this.mi.getOutData().put("GRP3", exttr1Data.getString("EXGRP3"));
			this.mi.getOutData().put("GRP4", exttr1Data.getString("EXGRP4"));
			this.mi.getOutData().put("GRP5", exttr1Data.getString("EXGRP5"));
			this.mi.getOutData().put("NBOF", Math.round(nbof).toString());
			this.mi.getOutData().put("TIGE", exttr1Data.getString("EXTIGE"));
			this.mi.getOutData().put("NMAT", nmat.toString());
			if(!mtn1.isBlank()) {
				this.mi.getOutData().put("MTN1", mtn1);
				this.mi.getOutData().put("REQ1", req1.toString());
			}
			if(req2> 0) {
				this.mi.getOutData().put("MTN2", mtn2);
				this.mi.getOutData().put("REQ2", req2.toString());
			}
			this.mi.getOutData().put("STDT", exttr1Data.get("EXSTDT").toString());
			this.mi.getOutData().put("DESS", dess.toString());
			this.mi.getOutData().put("STYL", exttr1Data.getString("EXGRP1").trim()+" "+exttr1Data.getString("EXGRP2").trim()+" "+exttr1Data.getString("EXGRP3").trim()+" "+exttr1Data.getString("EXGRP4").trim());
			this.mi.getOutData().put("QDEB", "0");//TODO calcul
			this.mi.getOutData().put("RPQT", (rpq1+rpq2).toString());
			this.mi.getOutData().put("ITDS", exttr1Data.getString("EXITDS"));
			this.mi.getOutData().put("SCHS", schs.toString());
			this.mi.getOutData().put("UPDT", lastUpd);
			this.mi.getOutData().put("WOST", exttr1Data.getString("EXWOST"));
		} );
		if(nbRead > 0) {
			this.mi.write();
		}
	}

	/**
	 *  Add default value for new record.
	 * @param insertedRecord
	 * @param prefix The column prefix of the table.
	 */
	private void insertTrackingField(DBContainer insertedRecord, String prefix) {
		insertedRecord.set(prefix+"RGDT", (Integer) this.utility.call("DateUtil", "currentDateY8AsInt"));
		insertedRecord.set(prefix+"LMDT", (Integer) this.utility.call("DateUtil", "currentDateY8AsInt"));
		insertedRecord.set(prefix+"CHID", this.program.getUser());
		insertedRecord.set(prefix+"RGTM", (Integer) this.utility.call("DateUtil", "currentTimeAsInt"));
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
		updatedRecord.set(prefix+"LMDT", (Integer) this.utility.call("DateUtil", "currentDateY8AsInt"));
		updatedRecord.set(prefix+"CHID", this.program.getUser());
		updatedRecord.setInt(prefix+"CHNO", CHNO);
	}

}
