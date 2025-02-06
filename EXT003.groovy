/**
 * README
 *
 * Name: EXT003
 * Description: regroupe POF des articles Fashion sur des notes mères
 * Date                         Changed By                         Description
 * 20240711                     ddecosterd@hetic3.fr     	création
 */

public class EXT003 extends ExtendM3Batch {
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;
	private final MICallerAPI miCaller;

	private String errorMessage = "";

	private boolean capping = false;
	private int defaultNPPN = 10;
	private int defaultNSPN = 3;
	private List<double[]> corresFomrStock = [];

	public EXT003(ProgramAPI program, DatabaseAPI database, UtilityAPI utility, MICallerAPI miCaller) {
		this.program = program;
		this.database = database;
		this.utility = utility;
		this.miCaller = miCaller;
	}

	public void main() {
		Integer CONO = program.getLDAZD().CONO;

		DBAction cugex1Record = database.table("CUGEX1").index("00").selection("F1A030","F1CHB1","F1CHB2").build();
		DBContainer cugex1Container = cugex1Record.createContainer();
		cugex1Container.setInt("F1CONO", CONO);
		cugex1Container.setString("F1FILE", "BATCH");
		cugex1Container.setString("F1PK01", "EXT003");

		if(!cugex1Record.read(cugex1Container))
			return;

		String  FACI = cugex1Container.getString("F1A030");

		if(!checkInputs(CONO, FACI))
			return;

		init(CONO);

		resetTables(CONO);

		qualifOF(CONO, FACI);

		fillEXTWR0(CONO, FACI);

		fillWr3Wr2(CONO, FACI);

		calculTectRest(CONO, FACI);

		if(!fillEXNOT(CONO, FACI)) {
			clearTableNeed(CONO);
			return;
		}

		creationNoteMerePrioritaire(CONO);

		if(!creationNoteMere(CONO)) {
			clearTableNeed(CONO);
			return;
		}
		clearTableNeed(CONO);
	}

	/**
	 * Check input values
	 * @param cono
	 * @param faci
	 * @return true if all green.
	 */
	private boolean checkInputs(Integer cono, String  faci) {
		if(cono == null) {
			miCaller.call("EXT001MI","AddError", ["CONO":cono.toString(),"IFID":"BATCH","FILE":"EXT003", "ERRM":"La division est obligatoire."],{});
			return false;
		}
		if(!utility.call("CheckUtil", "checkConoExist", database, cono)) {
			miCaller.call("EXT001MI","AddError", ["CONO":cono.toString(),"IFID":"BATCH","FILE":"EXT003", "ERRM":"La division "+cono+" est inexistant."],{});
			return false;
		}

		if(faci.isEmpty()) {
			miCaller.call("EXT001MI","AddError", ["CONO":cono.toString(),"IFID":"BATCH","FILE":"EXT003", "ERRM":"L'établissement est obligatoire. Vérifiez le paramétrage CUGEX01."],{});
			return false;
		}
		if(!utility.call("CheckUtil", "checkFacilityExist", database, cono, faci)) {
			miCaller.call("EXT001MI","AddError", ["CONO":cono.toString(),"IFID":"BATCH","FILE":"EXT003", "ERRM":"L'établissement "+faci+" est inexistant."],{});
			return false;
		}

		return true;
	}

	/**
	 * Empty tables
	 * @param cono
	 */
	private void resetTables(Integer cono) {

		DBAction wr0Record = database.table("EXTWR0").index("00").build();
		DBContainer wr0Container = wr0Record.createContainer();
		wr0Container.setInt("EXCONO", cono);
		wr0Record.readAll(wr0Container,1,30000,{DBContainer wr0Data ->
			DBAction wr0UpdateRecord = database.table("EXTWR0").index("00").build();
			DBContainer wr0UpdateContainer = wr0UpdateRecord.createContainer();
			wr0UpdateContainer.setInt("EXCONO", wr0Data.getInt("EXCONO"));
			wr0UpdateContainer.setString("EXFACI", wr0Data.getString("EXFACI"));
			wr0UpdateContainer.setString("EXHDPR", wr0Data.getString("EXHDPR"));
			wr0UpdateContainer.setString("EXPRNO", wr0Data.getString("EXPRNO"));
			wr0UpdateContainer.setInt("EXPLPN", wr0Data.getInt("EXPLPN"));
			wr0UpdateContainer.setInt("EXPLDT", wr0Data.getInt("EXPLDT"));
			wr0UpdateRecord.readLock(wr0UpdateContainer, {LockedResult entry ->
				entry.delete();
			});
		});

		DBAction wr3Record = database.table("EXTWR3").index("00").build();
		DBContainer wr3Container = wr3Record.createContainer();
		wr3Container.setInt("EXCONO", cono);
		wr3Record.readAll(wr3Container,1,15000,{DBContainer wr3Data ->
			DBAction wr3UpdateRecord = database.table("EXTWR3").index("00").build();
			DBContainer wr3UpdateContainer = wr3UpdateRecord.createContainer();
			wr3UpdateContainer.setInt("EXCONO", wr3Data.getInt("EXCONO"));
			wr3UpdateContainer.setString("EXFACI", wr3Data.getString("EXFACI"));
			wr3UpdateContainer.setString("EXHDPR", wr3Data.getString("EXHDPR"));
			wr3UpdateContainer.setString("EXPRNO", wr3Data.getString("EXPRNO"));
			wr3UpdateRecord.readLock(wr3UpdateContainer, {LockedResult entry ->
				entry.delete();
			});
		});

		DBAction wr2Record = database.table("EXTWR2").index("00").build();
		DBContainer wr2Container = wr2Record.createContainer();
		wr2Container.setInt("EXCONO", cono);
		wr2Record.readAll(wr2Container,1,5000,{DBContainer wr2Data ->
			DBAction wr2UpdateRecord = database.table("EXTWR2").index("00").build();
			DBContainer wr2UpdateContainer = wr2UpdateRecord.createContainer();
			wr2UpdateContainer.setInt("EXCONO", wr2Data.getInt("EXCONO"));
			wr2UpdateContainer.setString("EXFACI", wr2Data.getString("EXFACI"));
			wr2UpdateContainer.setString("EXHDPR", wr2Data.getString("EXHDPR"));
			wr2UpdateRecord.readLock(wr2UpdateContainer, {LockedResult entry ->
				entry.delete();
			});
		});

		DBAction notRecord = database.table("EXTNOT").index("00").build();
		DBContainer notContainer = notRecord.createContainer();
		notContainer.setInt("EXCONO", cono);
		notRecord.readAll(notContainer,1,6000,{DBContainer notData ->
			DBAction notUpdateRecord = database.table("EXTNOT").index("00").build();
			DBContainer notUpdateContainer = notUpdateRecord.createContainer();
			notUpdateContainer.setInt("EXCONO", notData.getInt("EXCONO"));
			notUpdateContainer.setLong("EXSCHN", notData.getLong("EXSCHN"));
			notUpdateRecord.readLock(notUpdateContainer, {LockedResult entry ->
				entry.delete();
			});
		});

	}

	/**
	 * Emptying the table of calculated needs.
	 * @param cono
	 */
	private void clearTableNeed(Integer cono) {
		DBAction besRecord = database.table("EXTBES").index("00").build();
		DBContainer besContainer = besRecord.createContainer();
		besContainer.setInt("EXCONO", cono);
		besRecord.readAll(besContainer,1,50000,{DBContainer besData ->
			DBAction besUpdateRecord = database.table("EXTBES").index("00").build();
			DBContainer besUpdateContainer = besUpdateRecord.createContainer();
			besUpdateContainer.setInt("EXCONO", besData.getInt("EXCONO"));
			besUpdateContainer.setString("EXFACI", besData.getString("EXFACI"));
			besUpdateContainer.setString("EXPRNO", besData.getString("EXPRNO"));
			besUpdateRecord.readLock(besUpdateContainer, {LockedResult entry ->
				entry.delete();
			});
		});
	}

	/**
	 * Get configuration values
	 * @param cono
	 */
	private void init(int cono) {
		DBAction CUGEX1Record = database.table("CUGEX1").index("00").selection("F1N096","F1N196","F1CHB1").build();
		DBContainer CUGEX1Container = CUGEX1Record.createContainer();
		CUGEX1Container.setInt("F1CONO", cono);
		CUGEX1Container.setString("F1FILE", "EXT003");
		CUGEX1Container.setString("F1PK01", "DEFAUT");
		if(CUGEX1Record.read(CUGEX1Container)) {
			capping = CUGEX1Container.getInt("F1CHB1") == 1;
			defaultNPPN = CUGEX1Container.get("F1N096");
			defaultNSPN = CUGEX1Container.get("F1N196");
		}

		ExpressionFactory CUGEXExpressionFactory = database.getExpressionFactory("CUGEX1");
		CUGEXExpressionFactory =  CUGEXExpressionFactory.between("F1PK02","10","100");
		CUGEX1Record = database.table("CUGEX1").index("00").matching(CUGEXExpressionFactory).selection("F1N096","F1N196").build();
		CUGEX1Container = CUGEX1Record.createContainer();
		CUGEX1Container.setString("F1FILE", "EXT003");
		CUGEX1Container.setString("F1PK01", "NSPN");
		CUGEX1Record.readAll(CUGEX1Container, 2, {
			corresFomrStock.push([CUGEX1Container.getDouble("F1N096"),CUGEX1Container.getDouble("F1N196")] as double[]);
		});
	}

	/**
	 * Add default value for new record.
	 * @param insertedRecord
	 */
	private void insertTrackingField(DBContainer insertedRecord) {
		insertedRecord.set("EXRGDT", (Integer) utility.call("DateUtil", "currentDateY8AsInt"));
		insertedRecord.set("EXLMDT", (Integer) utility.call("DateUtil", "currentDateY8AsInt"));
		insertedRecord.set("EXCHID", program.getUser());
		insertedRecord.set("EXRGTM", (Integer) utility.call("DateUtil", "currentTimeAsInt"));
		insertedRecord.set("EXCHNO", 1);
	}

	/**
	 * Add default value for updated record.
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
	 * Fill EXTWR0 with values from MMOPLP
	 * @param cono
	 * @param faci
	 */
	private void fillEXTWR0(Integer cono, String faci) {
		boolean noError = true;
		ExpressionFactory MMOPLPExpressionFactory = database.getExpressionFactory("MMOPLP");
		MMOPLPExpressionFactory =  MMOPLPExpressionFactory.ne("ROHDPR","");

		DBAction MMOPLPRecord = database.table("MMOPLP").index("10").matching(MMOPLPExpressionFactory).selection("ROCONO","ROFACI","ROHDPR","ROPRNO","ROPLPN","ROPLDT").build();
		DBContainer MMOPLPContainer = MMOPLPRecord.createContainer();
		MMOPLPContainer.setInt("ROCONO", cono);
		MMOPLPContainer.setString("ROFACI", faci);


		Closure<Boolean> MMOPLPClosure = { DBContainer MMOPLPdata ->
			int NPPN = 0;
			int NSPN = 0;
			DBAction ITMASRecord = database.table("MITMAS").index("00").selection("MMGRP1","MMGRP2","MMGRP3","MMTPLI","MMHDPR","MMGRTI").build();
			DBContainer ITMASContainer = ITMASRecord.createContainer();
			ITMASContainer.setInt("MMCONO", cono);
			ITMASContainer.setString("MMITNO", MMOPLPdata.getString("ROPRNO"));
			if(!ITMASRecord.read(ITMASContainer)) {
				miCaller.call("EXT001MI","AddError", ["CONO":cono.toString(),"IFID":"BATCH","FILE":"EXT003", "ERRM":"Article "+MMOPLPdata.getString("ROPRNO")+" non trouvé"],{});
				noError = false;
				return;
			}

			DBAction ITMAHRecord = database.table("MITMAH").index("00").selection("HMTX15","HMTY15").build();
			DBContainer ITMAHContainer = ITMAHRecord.createContainer();
			ITMAHContainer.setInt("HMCONO", cono);
			ITMAHContainer.setString("HMITNO", MMOPLPdata.getString("ROPRNO"));
			if(ITMAHRecord.read(ITMAHContainer)) {

				NPPN = getNPPN(cono, ITMASContainer.getString("MMTPLI"), ITMASContainer.getString("MMGRP1"), ITMASContainer.getString("MMGRP2"), ITMASContainer.getString("MMGRP3"),
						ITMASContainer.getString("MMGRTI"));
				NSPN = getNSPN(cono, MMOPLPdata.getString("ROPRNO"), ITMASContainer.getString("MMTPLI"), ITMASContainer.getString("MMHDPR"), ITMAHContainer.getString("HMTX15"),
						ITMAHContainer.getString("HMTY15"), ITMASContainer.getString("MMGRTI"));

				DBAction wr0Record = database.table("EXTWR0").index("00").build();
				DBContainer wr0Container = wr0Record.createContainer();
				wr0Container.set("EXCONO", MMOPLPdata.get("ROCONO"));
				wr0Container.set("EXFACI", MMOPLPdata.get("ROFACI"));
				wr0Container.set("EXHDPR", MMOPLPdata.get("ROHDPR"));
				wr0Container.set("EXPRNO", MMOPLPdata.get("ROPRNO"));
				wr0Container.set("EXPLPN", MMOPLPdata.get("ROPLPN"));
				wr0Container.set("EXSCHN", 0);
				wr0Container.setInt("EXORIG", getOriginPOF(MMOPLPdata.getInt("ROCONO"), MMOPLPdata.getString("ROFACI"), MMOPLPdata.getString("ROPRNO")));
				wr0Container.set("EXPLDT", MMOPLPdata.get("ROPLDT"));
				wr0Container.set("EXNPPN", NPPN);
				wr0Container.set("EXNSPN", NSPN);

				insertTrackingField(wr0Container);

				if(!wr0Record.insert(wr0Container)) {
					miCaller.call("EXT001MI","AddError", ["CONO":cono.toString(),"IFID":"BATCH","FILE":"EXT003", "ERRM":"Record CONO:"+MMOPLPdata.get("ROCONO")+" FACI:"+MMOPLPdata.get("ROFACI")+" HDPR:"+MMOPLPdata.get("ROHDPR")+" PRNO:"+MMOPLPdata.get("ROPRNO")+" PLPN:"+MMOPLPdata.get("ROPLPN")+" already exist"],{});
				}
			}

		}

		MMOPLPRecord.readAll(MMOPLPContainer, 2, 30000, MMOPLPClosure);
	}

	/**
	 * Lit la table CUGEX1 FILE=PRD001 et PK01=NPPN pour le paramétrage du nombre d'OF par note.
	 * @param cono
	 * @param tpli
	 * @param grp1
	 * @param grp2
	 * @param grp3
	 * @param grti
	 * @return nombre d'OF maxi par note.
	 */
	private int getNPPN(Integer cono, String tpli, String grp1, String grp2, String grp3, String grti) {
		int NPPN = 0;

		DBAction CUGEX1Record = database.table("CUGEX1").index("00").selection("F1N096","F1N196").build();
		DBContainer CUGEX1Container = CUGEX1Record.createContainer();
		CUGEX1Container.setInt("F1CONO", cono);
		CUGEX1Container.setString("F1FILE", "EXT003");
		CUGEX1Container.setString("F1PK01",  'NPPN');
		CUGEX1Container.setString("F1PK02",  'STYLE');
		CUGEX1Container.setString("F1PK03", tpli );
		if(CUGEX1Record.read(CUGEX1Container)) {
			NPPN = CUGEX1Container.get("F1N096");
		}
		if(NPPN == 0) {
			CUGEX1Container.setString("F1PK02",  'TIGE');
			CUGEX1Container.setString("F1PK03", grp2 );
			if(CUGEX1Record.read(CUGEX1Container)) {
				NPPN = CUGEX1Container.get("F1N096");
			}
		}
		if(NPPN == 0) {
			CUGEX1Container.setString("F1PK02",  'FORME_ET_PATRONNAGE');
			CUGEX1Container.setString("F1PK03", grp1 );
			CUGEX1Container.setString("F1PK04", grp3 );
			if(CUGEX1Record.read(CUGEX1Container)) {
				NPPN = CUGEX1Container.get("F1N096");
			}
		}
		CUGEX1Container.setString("F1PK04", "" );
		if(NPPN == 0) {
			CUGEX1Container.setString("F1PK02",  'FORME');
			CUGEX1Container.setString("F1PK03", grp1 );
			if(CUGEX1Record.read(CUGEX1Container)) {
				NPPN = CUGEX1Container.get("F1N096");
			}
		}
		if(NPPN == 0) {
			CUGEX1Container.setString("F1PK02",  'GRP_TECHNO');
			CUGEX1Container.setString("F1PK03", grti );
			if(CUGEX1Record.read(CUGEX1Container)) {
				NPPN = CUGEX1Container.get("F1N096");
			}
		}
		if(NPPN == 0) {
			NPPN = defaultNPPN;
			miCaller.call("EXT001MI","AddError", ["CONO":cono.toString(),"IFID":"BATCH","FILE":"EXT003", "ERRM":"La valeur de DEFAUT1 ("+NPPN+") a été utilisé pour le nombre d'OF maxi par note."],{});
		}
		return NPPN;
	}

	/**
	 * Lit la table CUGEX1 FILE=PRD001 et PK01=NSPN pour le paramétrage du nombre de SKU identique par note.
	 * @param cono
	 * @param prno
	 * @param tpli
	 * @param hdpr
	 * @param TX15
	 * @param TY15
	 * @param grti
	 * @return nombre de SKU identique par note.
	 */
	private int getNSPN(Integer cono, String prno, String tpli, String hdpr, String TX15, String TY15, String grti) {
		int NSPN = 0;
		DBAction CUGEX1Record = database.table("CUGEX1").index("00").selection("F1N096","F1N196").build();
		DBContainer CUGEX1Container = CUGEX1Record.createContainer();
		CUGEX1Container.setInt("F1CONO", cono);
		CUGEX1Container.setString("F1FILE", "EXT003");
		CUGEX1Container.setString("F1PK01",  'NSPN');
		CUGEX1Container.setString("F1PK02",  'SKU');
		CUGEX1Container.setString("F1PK03", prno );
		if(CUGEX1Record.read(CUGEX1Container)) {
			NSPN = CUGEX1Container.get("F1N096");
		}
		if(NSPN == 0) {
			CUGEX1Container.setString("F1PK02",  'STYLE');
			CUGEX1Container.setString("F1PK03", tpli );
			if(CUGEX1Record.read(CUGEX1Container)) {
				NSPN = CUGEX1Container.get("F1N096");
			}
		}
		if(NSPN == 0 && !corresFomrStock.empty) {
			DBAction mitmasRecord = database.table("MITMAS").index("00").selection("MMGRP1").build();
			DBContainer mitmasContainer = mitmasRecord.createContainer();
			mitmasContainer.setInt("MMCONO", cono);
			mitmasContainer.setString("MMITNO", hdpr);
			if(mitmasRecord.read(mitmasContainer)) {
				DBAction mitbalRecord = database.table("MITBAL").index("00").selection("MBSTQT").build();
				DBContainer mitbalContainer = mitbalRecord.createContainer();
				mitbalContainer.setInt("MBCONO", cono);
				mitbalContainer.setString("MBWHLO", "FMA");
				mitbalContainer.setString("MBITNO", hdpr.substring(0, 5)+mitmasContainer.getString("MMGRP1")+"STD"+TX15+TY15);
				if(mitbalRecord.read(mitbalContainer)) {
					double stock = mitbalContainer.getDouble("MBSTQT");
					for(corres in corresFomrStock) {

						if(stock <= corres[0]) {
							NSPN = corres[1].intValue();
							break;
						}
					}
					if(NSPN == 0) {
						NSPN = 7;
					}
				}
			}
		}
		if(NSPN == 0) {
			CUGEX1Container.setString("F1PK02",  'GRP_TECHNO');
			CUGEX1Container.setString("F1PK03", grti );
			if(CUGEX1Record.read(CUGEX1Container)) {
				NSPN = CUGEX1Container.get("F1N096");
			}
		}
		if(NSPN == 0) {
			NSPN = defaultNSPN;
			miCaller.call("EXT001MI","AddError", ["CONO":cono.toString(),"IFID":"BATCH","FILE":"EXT003", "ERRM":"La valeur de DEFAUT2 ("+NSPN+") a été utilisé pour le nombre de SKU identique par note."],{});
		}
		return NSPN;
	}

	/**
	 * Fill EXTWR2(EXTWR0 group by HDPR) and EXTWR3(EXTWR0 group by HDPR and PRNO)
	 * @param cono
	 * @param faci
	 */
	private void fillWr3Wr2(Integer cono, String faci) {
		DBAction wr0Record = database.table("EXTWR0").index("00").selection("EXORIG","EXNPPN","EXNSPN").build();
		DBContainer wr0Container = wr0Record.createContainer();
		wr0Container.set("EXCONO", cono);
		wr0Container.set("EXFACI", faci);

		Closure<Boolean> WR0Closure = { DBContainer WR0data ->

			DBAction wr3Record = database.table("EXTWR3").index("00").build();
			DBContainer wr3Container = wr3Record.createContainer();
			wr3Container.set("EXCONO", WR0data.get("EXCONO"));
			wr3Container.set("EXFACI", WR0data.get("EXFACI"));
			wr3Container.set("EXHDPR", WR0data.get("EXHDPR"));
			wr3Container.set("EXPRNO", WR0data.get("EXPRNO"));

			if(!wr3Record.readLock(wr3Container){LockedResult updatedRecord ->
						if(updatedRecord.getInt("EXORIG")==0) {
							updatedRecord.setInt("EXORIG", WR0data.getInt("EXORIG"));
						}
						updatedRecord.setInt("EXTSKU", updatedRecord.getInt("EXTSKU") + 1);
						updateTrackingField(updatedRecord, "EX");
						updatedRecord.update();
					}) {

				wr3Container.set("EXORIG", WR0data.get("EXORIG"));
				wr3Container.set("EXNPPN", WR0data.get("EXNPPN"));
				wr3Container.set("EXNSPN", WR0data.get("EXNSPN"))
				wr3Container.set("EXNNOT", 0);
				wr3Container.set("EXTSKU", 1)
				wr3Container.set("EXTECT", 0)
				wr3Container.set("EXREST", 0)

				insertTrackingField(wr3Container);

				wr3Record.insert(wr3Container);
			}

			DBAction wr2Record = database.table("EXTWR2").index("00").build();
			DBContainer wr2Container = wr2Record.createContainer();
			wr2Container.set("EXCONO", WR0data.get("EXCONO"));
			wr2Container.set("EXFACI", WR0data.get("EXFACI"));
			wr2Container.set("EXHDPR", WR0data.get("EXHDPR"));

			if(!wr2Record.readLock(wr2Container,{LockedResult updatedRecord ->
						updatedRecord.set("EXTSTY", updatedRecord.getInt("EXTSTY") + 1);
						updateTrackingField(updatedRecord, "EX");
						updatedRecord.update();
					})) {

				wr2Container.set("EXNPPN", WR0data.get("EXNPPN"));
				wr2Container.set("EXNSPN", WR0data.get("EXNSPN"))
				wr2Container.set("EXNNOT", 0);
				wr2Container.set("EXTSTY", 1)

				insertTrackingField(wr2Container);

				wr2Record.insert(wr2Container);
			}
		}

		wr0Record.readAll(wr0Container, 2, 30000, WR0Closure);
	}

	/**
	 * Calculate value for EXTECT and EXTREST in EXTWR3 
	 * @param cono
	 * @param faci
	 */
	private void calculTectRest(Integer cono, String faci) {
		DBAction wr3Record = database.table("EXTWR3").index("01").selection("EXTSKU").build();
		DBContainer wr3Container = wr3Record.createContainer();
		wr3Container.set("EXCONO", cono);
		wr3Container.set("EXFACI", faci);

		Closure<Boolean> WR3Closure = { DBContainer wr3data ->
			DBAction wr2Record = database.table("EXTWR2").index("00").selection("EXTSTY", "EXNPPN", "EXNSPN","EXMNOT").build();
			DBContainer wr2Container = wr2Record.createContainer();
			wr2Container.set("EXCONO", wr3data.get("EXCONO"));
			wr2Container.set("EXFACI", wr3data.get("EXFACI"));
			wr2Container.set("EXHDPR", wr3data.get("EXHDPR"));

			Double NNOT = 0;
			Double MNOT = 0;
			wr2Record.readLock(wr2Container, {LockedResult updatedRecord ->
				Integer TSKY = updatedRecord.get("EXTSTY");
				Integer NPPN = updatedRecord.get("EXNPPN");
				NNOT = Math.ceil((TSKY/NPPN).toDouble());
				MNOT = Math.ceil((wr3data.getInt("EXTSKU")/updatedRecord.getInt("EXNSPN")).toDouble())
				if(updatedRecord.getInt("EXMNOT")<MNOT) {
					updatedRecord.set("EXMNOT", MNOT);
				}
				updatedRecord.set("EXNNOT", NNOT);
				updateTrackingField(updatedRecord, "EX");
				updatedRecord.update();
			});

			DBAction wr3RecordUpdate =database.table("EXTWR3").index("00").selection("EXTSKU").build();
			DBContainer wr3ContainerUpdate = wr3RecordUpdate.createContainer();
			wr3ContainerUpdate.set("EXCONO", wr3data.get("EXCONO"));
			wr3ContainerUpdate.set("EXFACI", wr3data.get("EXFACI"));
			wr3ContainerUpdate.set("EXHDPR", wr3data.get("EXHDPR"));
			wr3ContainerUpdate.set("EXPRNO", wr3data.get("EXPRNO"));

			wr3RecordUpdate.readLock(wr3ContainerUpdate,{LockedResult updatedRecord ->
				Double TECT =  updatedRecord.getInt("EXTSKU");
				if(capping)
					TECT = Math.min(NNOT, updatedRecord.getInt("EXTSKU"));
				updatedRecord.set("EXTECT", TECT);
				updatedRecord.set("EXREST", TECT);
				updateTrackingField(updatedRecord, "EX");
				updatedRecord.update();
			});
		}

		wr3Record.readAll(wr3Container, 2, 15000, WR3Closure);
	}

	/**
	 * Calculate the total number of 'note' 
	 * @param cono
	 * @param faci
	 * @return the total number of 'note' 
	 */
	private int getNbTotNot(Integer cono, String faci) {
		DBAction wr2Record = database.table("EXTWR2").index("00").selection("EXNNOT","EXMNOT").build();
		DBContainer wr2Container = wr2Record.createContainer();
		wr2Container.set("EXCONO", cono);
		wr2Container.set("EXFACI", faci);

		int nbTotNot = 0;
		Closure<Integer> WR2Closure = { DBContainer WR2data ->
			int nnot =  WR2data.getInt("EXNNOT");
			int mnot =  WR2data.getInt("EXMNOT");
			nbTotNot += nnot > mnot ? nnot : mnot;
		}

		wr2Record.readAll(wr2Container, 2, 5000, WR2Closure);
		return nbTotNot
	}

	/**
	 * Fill EXTNOT with usable schn.
	 * @param cono
	 * @param faci
	 * @return true if no error occured.
	 */
	private boolean fillEXNOT(Integer cono, String faci) {
		DBAction wr0Record = database.table("EXTWR0").index("02").build();
		DBContainer wr0Container = wr0Record.createContainer();
		wr0Container.setInt("EXCONO", cono);
		wr0Container.setString("EXFACI", faci);
		wr0Container.setInt("EXORIG", 0);
		int nbPrio = wr0Record.readAll(wr0Container,3, 3000, {});

		int nbTotNot = getNbTotNot(cono, faci) + nbPrio;
		long wwschn = 88880000000;
		Set<Long> usedSCHN = usedSCHN(cono, faci);
		Set<Long> usableSCHN = usableSCHN(cono, usedSCHN);

		while(nbTotNot > 0) {
			while(usedSCHN.contains(wwschn)) {
				wwschn++;
			}

			if(!usableSCHN.contains(wwschn)) {
				miCaller.call("PMS270MI","AddScheduleNo", ["SCHN":wwschn.toString(), "TX40":wwschn.toString()],{Map<String, String> response ->
					if(response.containsKey("error")) {
						miCaller.call("EXT001MI","AddError", ["CONO":cono.toString(),"IFID":"BATCH","FILE":"EXT003", "ERRM":"AddScheduleNo:"+response.get("errorMessage")],{});
						return false;
					}
				});
			}

			DBAction notRecord = database.table("EXTNOT").index("00").build();
			DBContainer notContainer = notRecord.createContainer();
			notContainer.setInt("EXCONO", cono);
			notContainer.setLong("EXSCHN", wwschn);
			notContainer.set("EXNPPN", 9999);
			notContainer.set("EXREST", 9999);
			notContainer.set("EXPLDT", 0);

			insertTrackingField(notContainer);

			notRecord.insert(notContainer);
			nbTotNot--;

			wwschn++;
		}

		return true;
	}

	/**
	 * Regroupement des of en notes mères. 
	 * @param cono
	 * @return true if there was no error.
	 */
	private boolean creationNoteMere(Integer cono) {
		boolean noError = true;
		boolean hasSkuNotUsed = true;

		ExpressionFactory EXTNOTExpressionFactory = database.getExpressionFactory("EXTNOT");
		EXTNOTExpressionFactory =  EXTNOTExpressionFactory.gt("EXREST","0");

		DBAction notRecord = database.table("EXTNOT").index("10").matching(EXTNOTExpressionFactory).selection("EXREST","EXPLDT").build();
		DBContainer notContainer = notRecord.createContainer();
		notContainer.setInt("EXCONO", cono);


		Closure<Integer> NOTClosure = { DBContainer NOTdata ->
			if(hasSkuNotUsed) {
				ExpressionFactory EXTNWR3ExpressionFactory = database.getExpressionFactory("EXTWR3");
				EXTNWR3ExpressionFactory =  EXTNWR3ExpressionFactory.gt("EXREST","0");

				DBAction wr3Record = database.table("EXTWR3").index("01").matching(EXTNWR3ExpressionFactory).selection("EXNPPN","EXNSPN","EXREST","EXORIG").build();
				DBContainer wr3Container = wr3Record.createContainer();
				wr3Container.set("EXCONO", cono);

				String FACI;
				String HDPR;
				int wr3NPPN = 0;

				int readed = wr3Record.readAll(wr3Container, 1, 1,{ DBContainer wr3data ->
					FACI = wr3data.getString("EXFACI");
					HDPR = 	wr3data.getString("EXHDPR");
					wr3NPPN = wr3data.getInt("EXNPPN");
				});

				//wee have finished the
				if(readed == 0) {
					hasSkuNotUsed = false;
					return;
				}

				int aProgNot = NOTdata.getInt("EXREST");
				int pldt = NOTdata.getInt("EXPLDT");

				//L'enregistrement n'a pas encore été utilisé
				if(aProgNot == 9999) {
					aProgNot = wr3NPPN;
					if(aProgNot > 10000) {
						miCaller.call("EXT001MI","AddError", ["CONO":cono.toString(),"IFID":"BATCH","FILE":"EXT003", "ERRM":"Le nombre de note par page ne peux excéder 10000."],{});
						return;
					}
					DBAction notURecord = database.table("EXTNOT").index("00").build();
					DBContainer notUContainer = notURecord.createContainer();
					notUContainer.setInt("EXCONO", cono);
					notUContainer.set("EXSCHN", NOTdata.get("EXSCHN"));
					notURecord.readLock(notUContainer,{LockedResult updatedNotRecord ->
						updatedNotRecord.setInt("EXNPPN", wr3NPPN);
						updatedNotRecord.setInt("EXREST", aProgNot);
						updateTrackingField(updatedNotRecord, "EX");
						updatedNotRecord.update();
					});
				}

				wr3Container.setString("EXFACI", FACI);
				wr3Container.setString("EXHDPR", HDPR);

				Closure<Boolean> WR3ReadClosure = { DBContainer wr3data ->
					if(aProgNot > 0) {
						int aProgSku = 0;

						if(wr3data.getInt("EXNSPN") > wr3data.getInt("EXREST"))
							aProgSku = wr3data.getInt("EXREST");
						else
							aProgSku = wr3data.getInt("EXNSPN");

						if(aProgSku >= aProgNot)
							aProgSku = aProgNot;

						if(aProgSku > 10000) {
							miCaller.call("EXT001MI","AddError", ["CONO":cono.toString(),"IFID":"BATCH","FILE":"EXT003", "ERRM":"Le nombre de SKU par page ne peux excéder 10000."],{});
							return;
						}

						DBAction wr3URecord = database.table("EXTWR3").index("00").build();
						DBContainer wr3UContainer = wr3URecord.createContainer();
						wr3UContainer.set("EXCONO", wr3data.get("EXCONO"));
						wr3UContainer.set("EXFACI", wr3data.get("EXFACI"));
						wr3UContainer.set("EXHDPR", wr3data.get("EXHDPR"));
						wr3UContainer.set("EXORIG", wr3data.get("EXORIG"));
						wr3UContainer.set("EXPRNO", wr3data.get("EXPRNO"));

						wr3URecord.readLock(wr3UContainer){LockedResult updatedRecord ->
							updatedRecord.set("EXREST", (updatedRecord.getInt("EXREST") - aProgSku));
							updateTrackingField(updatedRecord, "EX");
							updatedRecord.update();
						}

						DBAction wr0Record = database.table("EXTWR0").index("03").selection("EXORIG").build();
						DBContainer wr0Container = wr0Record.createContainer();
						wr0Container.setInt("EXCONO", wr3data.getInt("EXCONO"));
						wr0Container.setString("EXFACI", wr3data.getString("EXFACI"));
						wr0Container.setString("EXHDPR", wr3data.getString("EXHDPR"));
						wr0Container.setString("EXPRNO", wr3data.getString("EXPRNO"));
						wr0Container.setLong("EXSCHN", 0);

						wr0Record.readAll(wr0Container,5, aProgSku, {DBContainer wr0data ->
							DBAction update = database.table("EXTWR0").index("00").build();
							DBContainer wr0UpdateContainer = update.createContainer();
							wr0UpdateContainer.set("EXCONO", wr0data.get("EXCONO"));
							wr0UpdateContainer.set("EXFACI", wr0data.get("EXFACI"));
							wr0UpdateContainer.set("EXHDPR", wr0data.get("EXHDPR"));
							wr0UpdateContainer.set("EXPRNO", wr0data.get("EXPRNO"));
							wr0UpdateContainer.set("EXPLPN", wr0data.get("EXPLPN"));
							wr0UpdateContainer.set("EXPLDT", wr0data.get("EXPLDT"));

							update.readLock(wr0UpdateContainer,{LockedResult updatedRecord ->
								updatedRecord.set("EXSCHN", NOTdata.get("EXSCHN"));
								updateTrackingField(updatedRecord, "EX");
								updatedRecord.update();

								aProgNot --;

								if(pldt == 0){
									pldt = updatedRecord.getInt("EXPLDT");
									int currentDate = utility.call("DateUtil", "currentDateY8AsInt");
									if(pldt < currentDate) {
										pldt = currentDate;
									}
									DBAction notURecord = database.table("EXTNOT").index("00").build();
									DBContainer notUContainer = notURecord.createContainer();
									notUContainer.setInt("EXCONO", cono);
									notUContainer.set("EXSCHN", NOTdata.get("EXSCHN"));
									notURecord.readLock(notUContainer,{LockedResult updatedNotRecord ->
										updatedNotRecord.setInt("EXPLDT", pldt);
										updateTrackingField(updatedNotRecord, "EX");
										updatedNotRecord.update();
									});
								}

								DBAction moplpRecord = database.table("MMOPLP").index("00").build();
								DBContainer moplpContainer = moplpRecord.createContainer();
								moplpContainer.setInt("ROCONO", cono);
								moplpContainer.setInt("ROPLPN", updatedRecord.getInt("EXPLPN"));

								moplpRecord.readLock(moplpContainer,{ LockedResult updtedmoplp ->
									updtedmoplp.set("ROSCHN", NOTdata.get("EXSCHN"));
									updtedmoplp.setInt("ROFIDT", pldt);
									updtedmoplp.setChar("ROPRIP", wr0data.getInt("EXORIG").toString());
									updateTrackingField(updtedmoplp, "RO");
									updtedmoplp.update();
								});
							});
						})
						DBAction notURecord = database.table("EXTNOT").index("00").build();
						DBContainer notUContainer = notURecord.createContainer();
						notUContainer.setInt("EXCONO", cono);
						notUContainer.set("EXSCHN", NOTdata.get("EXSCHN"));
						notURecord.readLock(notUContainer,{LockedResult updatedNotRecord ->
							updatedNotRecord.setInt("EXREST", aProgNot);
							updateTrackingField(updatedNotRecord, "EX");
							updatedNotRecord.update();
						});
					}
				}


				//dans le pire des cas il faut aProgNot lignes pour remplir notre note
				wr3Record.readAll(wr3Container, 3, aProgNot, WR3ReadClosure);
			}
		}

		notRecord.readAll(notContainer, 1, 6000, NOTClosure);
		return noError;
	}

	/**
	 * Regroupement des of prioritaires en notes mères.
	 * @param cono
	 */
	private void creationNoteMerePrioritaire(Integer cono) {
		boolean needEmptyNote = false;

		ExpressionFactory EXTNOTExpressionFactory = database.getExpressionFactory("EXTNOT");
		EXTNOTExpressionFactory =  EXTNOTExpressionFactory.gt("EXREST","0");

		DBAction notRecord = database.table("EXTNOT").index("10").matching(EXTNOTExpressionFactory).selection("EXPLDT").build();
		DBContainer notContainer = notRecord.createContainer();
		notContainer.setInt("EXCONO", cono);


		Closure<Integer> NOTClosure = { DBContainer NOTdata ->
			DBAction wr0Record = database.table("EXTWR0").index("00").selection("EXFACI").build();
			DBContainer wr0Container = wr0Record.createContainer();
			wr0Container.set("EXCONO", cono);

			String FACI;
			wr0Record.readAll(wr0Container, 1, 1,{ DBContainer wr0data ->
				FACI = wr0data.getString("EXFACI");
			});

			wr0Record = database.table("EXTWR0").index("02").selection("EXSCHN").build();
			wr0Container.setString("EXFACI", FACI);
			wr0Container.setInt("EXORIG", 0);
			wr0Container.setLong("EXSCHN", 0);

			int pldt = NOTdata.getInt("EXPLDT");

			wr0Record.readAll(wr0Container,4,1,{DBContainer wr0Data ->

				String HDPR = 	wr0Container.getString("EXHDPR");

				ExpressionFactory EXTNWR3ExpressionFactory = database.getExpressionFactory("EXTWR3");
				EXTNWR3ExpressionFactory =  EXTNWR3ExpressionFactory.gt("EXREST","0");
				DBAction wr3Record = database.table("EXTWR3").index("00").matching(EXTNWR3ExpressionFactory).selection("EXREST").build();
				DBContainer wr3Container = wr3Record.createContainer();
				wr3Container.setInt("EXCONO", wr0Data.getInt("EXCONO"));
				wr3Container.setString("EXFACI", wr0Data.getString("EXFACI"));
				wr3Container.setString("EXHDPR", wr0Data.getString("EXHDPR"));
				wr3Container.setString("EXPRNO", wr0Data.getString("EXPRNO"));

				wr3Record.readLock(wr3Container){LockedResult updated3Record ->
					updated3Record.set("EXREST", (updated3Record.getInt("EXREST") - 1));
					updateTrackingField(updated3Record, "EX");
					updated3Record.update();
				}

				if(pldt == 0){
					pldt = wr0Data.getInt("EXPLDT");
					int currentDate = utility.call("DateUtil", "currentDateY8AsInt");
					if(pldt < currentDate) {
						pldt = currentDate;
					}
				}

				DBAction notURecord = database.table("EXTNOT").index("00").build();
				DBContainer notUContainer = notURecord.createContainer();
				notUContainer.setInt("EXCONO", cono);
				notUContainer.set("EXSCHN", NOTdata.get("EXSCHN"));
				notURecord.readLock(notUContainer,{LockedResult updatedNotRecord ->
					updatedNotRecord.setInt("EXNPPN", 1);
					updatedNotRecord.setInt("EXPLDT", pldt);
					updatedNotRecord.setInt("EXREST", 0);
					updateTrackingField(updatedNotRecord, "EX");
					updatedNotRecord.update();
				});

				DBAction moplpRecord = database.table("MMOPLP").index("00").build();
				DBContainer moplpContainer = moplpRecord.createContainer();
				moplpContainer.setInt("ROCONO", cono);
				moplpContainer.setInt("ROPLPN", wr0Data.getInt("EXPLPN"));

				moplpRecord.readLock(moplpContainer,{ LockedResult updtedmoplp ->
					updtedmoplp.set("ROSCHN", NOTdata.get("EXSCHN"));
					updtedmoplp.setInt("ROFIDT", pldt);
					updtedmoplp.setChar("ROPRIP",'0');
					updateTrackingField(updtedmoplp, "RO");
					updtedmoplp.update();
				});

				DBAction wr0URecord = database.table("EXTWR0").index("00").build();
				DBContainer wr0UContainer = wr0URecord.createContainer();
				wr0UContainer.setInt("EXCONO", wr0Data.getInt("EXCONO"));
				wr0UContainer.setString("EXFACI", wr0Data.getString("EXFACI"));
				wr0UContainer.setString("EXHDPR", wr0Data.getString("EXHDPR"));
				wr0UContainer.setString("EXPRNO", wr0Data.getString("EXPRNO"));
				wr0UContainer.setInt("EXPLPN", wr0Data.getInt("EXPLPN"));
				wr0UContainer.setInt("EXPLDT", wr0Data.getInt("EXPLDT"));
				wr0URecord.readLock(wr0UContainer,{ LockedResult updatedRecord ->
					updatedRecord.set("EXSCHN", NOTdata.get("EXSCHN"));
					updateTrackingField(updatedRecord, "EX");
					updatedRecord.update();
				});
			});
		}

		notRecord.readAll(notContainer, 1, 4000, NOTClosure);
	}

	/**
	 * Search for SCHN already used between 88880000000 and 88889999999
	 * @param cono
	 * @param faci
	 * @return a set of used SCHN
	 */
	private Set<Long> usedSCHN(Integer cono, String faci) {
		Set<Long> result = new HashSet<Long>();
		ExpressionFactory mwohedExpressionFactory = database.getExpressionFactory("MWOHED");
		mwohedExpressionFactory = mwohedExpressionFactory.ge("VHSCHN", "88880000000").and(mwohedExpressionFactory.le("VHSCHN", "88889999999"));
		DBAction mwohedRecord = database.table("MWOHED").index("00").matching(mwohedExpressionFactory).selection("VHSCHN").build();
		DBContainer mwohedContainer = mwohedRecord.createContainer();
		mwohedContainer.setInt("VHCONO", cono);
		mwohedContainer.setString("VHFACI", faci);

		mwohedRecord.readAll(mwohedContainer, 2, 10000, { DBContainer MWOHEDdata ->
			result.add(MWOHEDdata.getLong("VHSCHN"))
		});
		return result;
	}

	/**
	 * Search for SCHN already created and not used between 88880000000 and 88889999999
	 * @param cono
	 * @param usedSCHN the set of used SCHN
	 * @return a set of usable SCHN
	 */
	private Set<Long> usableSCHN(Integer cono, Set<Long> usedSCHN){
		Set<Long> result = new HashSet<Long>();
		ExpressionFactory mschmaExpressionFactory = database.getExpressionFactory("MSCHMA");
		mschmaExpressionFactory = mschmaExpressionFactory.ge("HSSCHN", "88880000000").and(mschmaExpressionFactory.le("HSSCHN", "88889999999"));
		DBAction mschmaRecord = database.table("MSCHMA").index("00").matching(mschmaExpressionFactory).selection("HSSCHN").build();
		DBContainer mschmaContainer = mschmaRecord.createContainer();
		mschmaContainer.setInt("HSCONO", cono);

		mschmaRecord.readAll(mschmaContainer, 1, 10000, { DBContainer MSCHMAdata ->
			long schn = MSCHMAdata.getLong("HSSCHN");
			if(!usedSCHN.contains(schn)) {
				result.add(schn);
			}
		});
		return result;
	}

	/**
	 * Set priority on new fabrication orders.
	 * @param cono
	 * @param faci
	 */
	private void qualifOF(int cono, String faci) {
		DBAction extbesRecord = database.table("EXTBES").index("00").selection("EXTRQ0","EXTRQ1","EXTRQ2","EXTRQ3","EXTRQ4","EXTRQ5","EXTRQ6","EXTRQ7","EXTRQ8","EXTRQ9").build();
		DBContainer extbesContainer = extbesRecord.createContainer();
		extbesContainer.setInt("EXCONO", cono);
		extbesContainer.setString("EXFACI", faci);

		extbesRecord.readAll(extbesContainer, 2, 50000, { DBContainer extbesData ->
			DBAction extbesUpdateRecord = database.table("EXTBES").index("00").selection("EXTRQ0","EXTRQ1","EXTRQ2","EXTRQ3","EXTRQ4","EXTRQ5","EXTRQ6","EXTRQ7","EXTRQ8","EXTRQ9").build();
			DBContainer extbesUpdateContainer = extbesUpdateRecord.createContainer();
			extbesUpdateContainer.setInt("EXCONO", extbesData.getInt("EXCONO"));
			extbesUpdateContainer.setString("EXFACI", extbesData.getString("EXFACI"));
			extbesUpdateContainer.setString("EXPRNO", extbesData.getString("EXPRNO"));

			extbesUpdateRecord.readLock(extbesContainer, { LockedResult extbesUpdate ->

				ExpressionFactory mwohedExpressionFactory = database.getExpressionFactory("MWOHED");
				mwohedExpressionFactory = mwohedExpressionFactory.lt("VHWHST", "90");

				DBAction mwohedRecord = database.table("MWOHED").index("00").matching(mwohedExpressionFactory).selection("VHSCHN", "VHMAQT", "VHORQT", "VHPRNO").build();
				DBContainer mwohedContainer = mwohedRecord.createContainer();
				mwohedContainer.setInt("VHCONO", cono);
				mwohedContainer.setString("VHFACI", faci);
				mwohedContainer.setString("VHPRNO", extbesUpdate.getString("EXPRNO"));
				boolean besUpdated = false;

				mwohedRecord.readAll(mwohedContainer, 3, 100, {  DBContainer mwohedData ->
					if(mwohedData.getDouble("VHMAQT") >= mwohedData.getDouble("VHORQT"))
						return;
					String schn = ""+mwohedData.getLong("VHSCHN");
					if(schn != mwohedData.getString("VHMFNO")) {
						int prio = -1;
						if(extbesUpdate.getDouble("EXTRQ0")> 0) {
							extbesUpdate.setDouble("EXTRQ0", extbesUpdate.getDouble("EXTRQ0") - mwohedData.getDouble("VHORQT"));
							prio = 0;
							besUpdated = true;
						}else if(extbesUpdate.getDouble("EXTRQ1")> 0) {
							extbesUpdate.setDouble("EXTRQ1", extbesUpdate.getDouble("EXTRQ1") - mwohedData.getDouble("VHORQT"));
							prio = 1;
							besUpdated = true;
						}else if(extbesUpdate.getDouble("EXTRQ2")> 0) {
							extbesUpdate.setDouble("EXTRQ2", extbesUpdate.getDouble("EXTRQ2") - mwohedData.getDouble("VHORQT"));
							prio = 2;
							besUpdated = true;
						}else if(extbesUpdate.getDouble("EXTRQ3")> 0) {
							extbesUpdate.setDouble("EXTRQ3", extbesUpdate.getDouble("EXTRQ3") - mwohedData.getDouble("VHORQT"));
							prio = 3;
							besUpdated = true;
						}else if(extbesUpdate.getDouble("EXTRQ4")> 0) {
							extbesUpdate.setDouble("EXTRQ4", extbesUpdate.getDouble("EXTRQ4") - mwohedData.getDouble("VHORQT"));
							prio = 4;
							besUpdated = true;
						}else if(extbesUpdate.getDouble("EXTRQ5")> 0) {
							extbesUpdate.setDouble("EXTRQ5", extbesUpdate.getDouble("EXTRQ5") - mwohedData.getDouble("VHORQT"));
							prio = 5;
							besUpdated = true;
						}else if(extbesUpdate.getDouble("EXTRQ6")> 0) {
							extbesUpdate.setDouble("EXTRQ6", extbesUpdate.getDouble("EXTRQ6") - mwohedData.getDouble("VHORQT"));
							prio = 6;
							besUpdated = true;
						}else if(extbesUpdate.getDouble("EXTRQ7")> 0) {
							extbesUpdate.setDouble("EXTRQ7", extbesUpdate.getDouble("EXTRQ7") - mwohedData.getDouble("VHORQT"));
							prio = 7;
							besUpdated = true;
						}else if(extbesUpdate.getDouble("EXTRQ8")> 0) {
							extbesUpdate.setDouble("EXTRQ8", extbesUpdate.getDouble("EXTRQ8") - mwohedData.getDouble("VHORQT"));
							prio = 8;
							besUpdated = true;
						}else if(extbesUpdate.getDouble("EXTRQ9")> 0) {
							extbesUpdate.setDouble("EXTRQ9", extbesUpdate.getDouble("EXTRQ9") - mwohedData.getDouble("VHORQT"));
							prio = 9;
							besUpdated = true;
						}

						if(prio > -1) {
							DBAction mwohedUpdateRecord = database.table("MWOHED").index("00").selection("VHPRIO").build();
							DBContainer mwohedUpdateContainer = mwohedUpdateRecord.createContainer();
							mwohedUpdateContainer.setInt("VHCONO", mwohedData.getInt("VHCONO"));
							mwohedUpdateContainer.setString("VHFACI",mwohedData.getString("VHFACI"));
							mwohedUpdateContainer.setString("VHPRNO",mwohedData.getString("VHPRNO"));
							mwohedUpdateContainer.setString("VHMFNO",mwohedData.getString("VHMFNO"));
							mwohedUpdateRecord.readLock(mwohedUpdateContainer, {  LockedResult mwohedUpdate ->
								mwohedUpdate.setInt("VHPRIO", prio);
								updateTrackingField(mwohedUpdate, "VH");
								mwohedUpdate.update();
							});
						}
					}

				});

				if(besUpdated) {
					updateTrackingField(extbesUpdate, "EX");
					extbesUpdate.update();
				}
			});

		});
	}

	/**
	 * Get the highest priority aviable.
	 * @param cono
	 * @param faci
	 * @param prno
	 * @return priority
	 */
	private int getOriginPOF(int cono, String faci, String prno) {
		DBAction extbesRecord = database.table("EXTBES").index("00").selection("EXTRQ0","EXTRQ1","EXTRQ2","EXTRQ3","EXTRQ4","EXTRQ5","EXTRQ6","EXTRQ7","EXTRQ8","EXTRQ9").build();
		DBContainer extbesContainer = extbesRecord.createContainer();
		extbesContainer.setInt("EXCONO", cono);
		extbesContainer.setString("EXFACI", faci);
		extbesContainer.setString("EXPRNO", prno);
		int orig = 9;
		extbesRecord.readLock(extbesContainer, {  LockedResult EXTBESupdate ->
			boolean updated = false;
			if(EXTBESupdate.getDouble("EXTRQ0")> 0) {
				EXTBESupdate.setDouble("EXTRQ0", EXTBESupdate.getDouble("EXTRQ0") - 1);
				orig = 0;
				updated = true;
			}else if(EXTBESupdate.getDouble("EXTRQ1")> 0) {
				EXTBESupdate.setDouble("EXTRQ1", EXTBESupdate.getDouble("EXTRQ1") - 1);
				orig = 1;
				updated = true;
			}else if(EXTBESupdate.getDouble("EXTRQ2")> 0) {
				EXTBESupdate.setDouble("EXTRQ2", EXTBESupdate.getDouble("EXTRQ2") - 1);
				orig = 2;
				updated = true;
			}else if(EXTBESupdate.getDouble("EXTRQ3")> 0) {
				EXTBESupdate.setDouble("EXTRQ3", EXTBESupdate.getDouble("EXTRQ3") - 1);
				orig = 3;
				updated = true;
			}else if(EXTBESupdate.getDouble("EXTRQ4")> 0) {
				EXTBESupdate.setDouble("EXTRQ4", EXTBESupdate.getDouble("EXTRQ4") - 1);
				orig = 4;
				updated = true;
			}else if(EXTBESupdate.getDouble("EXTRQ5")> 0) {
				EXTBESupdate.setDouble("EXTRQ5", EXTBESupdate.getDouble("EXTRQ5") - 1);
				orig = 5;
				updated = true;
			}else if(EXTBESupdate.getDouble("EXTRQ6")> 0) {
				EXTBESupdate.setDouble("EXTRQ6", EXTBESupdate.getDouble("EXTRQ6") - 1);
				orig = 6;
				updated = true;
			}else if(EXTBESupdate.getDouble("EXTRQ7")> 0) {
				EXTBESupdate.setDouble("EXTRQ7", EXTBESupdate.getDouble("EXTRQ7") - 1);
				orig = 7;
				updated = true;
			}else if(EXTBESupdate.getDouble("EXTRQ8")> 0) {
				EXTBESupdate.setDouble("EXTRQ8", EXTBESupdate.getDouble("EXTRQ8") - 1);
				orig = 8;
				updated = true;
			}else if(EXTBESupdate.getDouble("EXTRQ9")> 9) {
				EXTBESupdate.setDouble("EXTRQ9", EXTBESupdate.getDouble("EXTRQ9") - 1);
				orig = 9;
				updated = true;
			}
			if(updated) {
				updateTrackingField(EXTBESupdate, "EX");
				EXTBESupdate.update();
			}
		});
		return orig;
	}
}