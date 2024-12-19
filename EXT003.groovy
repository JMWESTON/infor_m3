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
	private final LoggerAPI logger;

	private String errorMessage = "";

	private boolean capping = false;
	private int defaultNPPN = 10;
	private int defaultNSPN = 3;
	private List<double[]> corresFomrStock = [];

	public EXT003(ProgramAPI program, DatabaseAPI database, UtilityAPI utility, MICallerAPI miCaller, LoggerAPI logger) {
		this.program = program;
		this.database = database;
		this.utility = utility;
		this.miCaller = miCaller;
		this.logger = logger;
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
		logger.info("checkinput")

		init(CONO);

		resetTables(CONO, false);

		initOrig(CONO, FACI);

		logger.info("fillextwr0")
		fillEXTWR0(CONO, FACI);

		fillWR3_WR2(CONO, FACI);

		calculTectRest(CONO, FACI);

		if(!fillEXNOT(CONO, FACI)) {
			return;
		}
		logger.info("fillextnot")

		if(!creationNoteMere(CONO)) {
			return;
		}
		logger.info("creationnotemere")

		resetTables(CONO, true);
		logger.info("resetTable")


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
		if(!this.utility.call("CheckUtil", "checkConoExist", database, cono)) {
			miCaller.call("EXT001MI","AddError", ["CONO":cono.toString(),"IFID":"BATCH","FILE":"EXT003", "ERRM":"La division "+cono+" est inexistant."],{});
			return false;
		}

		if(faci.isEmpty()) {
			miCaller.call("EXT001MI","AddError", ["CONO":cono.toString(),"IFID":"BATCH","FILE":"EXT003", "ERRM":"L'établissement est obligatoire. Vérifiez le paramétrage CUGEX01."],{});
			return false;
		}
		if(!this.utility.call("CheckUtil", "checkFacilityExist", database, cono, faci)) {
			miCaller.call("EXT001MI","AddError", ["CONO":cono.toString(),"IFID":"BATCH","FILE":"EXT003", "ERRM":"L'établissement "+faci+" est inexistant."],{});
			return false;
		}

		return true;
	}

	/**
	 * Empty tables
	 * @param cono
	 * @param keepWr0
	 */
	private void resetTables(Integer cono, boolean keepWr0) {

		if(! keepWr0) {
			DBAction wr0Record = database.table("EXTWR0").index("00").build();
			DBContainer wr0Container = wr0Record.createContainer();
			wr0Container.setInt("EXCONO", cono);
			wr0Record.readAllLock(wr0Container,1,{LockedResult entry ->
				entry.delete();
			});
		}

		DBAction wr3Record = database.table("EXTWR3").index("00").build();
		DBContainer wr3Container = wr3Record.createContainer();
		wr3Container.setInt("EXCONO", cono);
		wr3Record.readAllLock(wr3Container,1,{LockedResult entry ->
			entry.delete();
		});

		DBAction wr2Record = database.table("EXTWR2").index("00").build();
		DBContainer wr2Container = wr2Record.createContainer();
		wr2Container.setInt("EXCONO", cono);
		wr2Record.readAllLock(wr2Container,1,{LockedResult entry ->
			entry.delete();
		});

		DBAction notRecord = database.table("EXTNOT").index("00").build();
		DBContainer notContainer = notRecord.createContainer();
		notContainer.setInt("EXCONO", cono);
		notRecord.readAllLock(notContainer,1,{LockedResult entry ->
			entry.delete();
		});

		DBAction besRecord = database.table("EXTBES").index("00").build();
		DBContainer besContainer = besRecord.createContainer();
		besContainer.setInt("EXCONO", cono);
		besRecord.readAllLock(besContainer,1,{LockedResult entry ->
			entry.delete();
		});
	}

	/**
	 * Get configuration values from CUGEX1
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
		}else {
			CUGEX1Container.setInt("F1CHB1", capping ? 1:0);
			CUGEX1Container.set("F1N096", defaultNPPN);
			CUGEX1Container.set("F1N196", defaultNSPN);

			CUGEX1Container.set("F1RGDT", (Integer) this.utility.call("DateUtil", "currentDateY8AsInt"));
			CUGEX1Container.set("F1LMDT", (Integer) this.utility.call("DateUtil", "currentDateY8AsInt"));
			CUGEX1Container.set("F1CHID", this.program.getUser());
			CUGEX1Container.set("F1RGTM", (Integer) this.utility.call("DateUtil", "currentTimeAsInt"));
			CUGEX1Container.set("F1CHNO", 1);
			CUGEX1Record.insert(CUGEX1Container);
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
		insertedRecord.set("EXRGDT", (Integer) this.utility.call("DateUtil", "currentDateY8AsInt"));
		insertedRecord.set("EXLMDT", (Integer) this.utility.call("DateUtil", "currentDateY8AsInt"));
		insertedRecord.set("EXCHID", this.program.getUser());
		insertedRecord.set("EXRGTM", (Integer) this.utility.call("DateUtil", "currentTimeAsInt"));
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
		updatedRecord.set(prefix+"LMDT", (Integer) this.utility.call("DateUtil", "currentDateY8AsInt"));
		updatedRecord.set(prefix+"CHID", this.program.getUser());
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


		Closure<Boolean> MMOPLPClosure = { DBContainer  MMOPLPdata ->
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

		MMOPLPRecord.readAll(MMOPLPContainer, 2, MMOPLPClosure);
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
	 * Fill EXTWR2(EXTWR0 group by HDPR) and EXWR3(EXTWR0 group by HDPR and PRNO)
	 * @param cono
	 * @param faci
	 */
	private void fillWR3_WR2(Integer cono, String faci) {
		DBAction wr0Record = database.table("EXTWR0").index("00").selectAllFields().build();
		DBContainer wr0Container = wr0Record.createContainer();
		wr0Container.set("EXCONO", cono);
		wr0Container.set("EXFACI", faci);

		Closure<Boolean> WR0Closure = { DBContainer WR0data ->

			DBAction wr3Record = this.database.table("EXTWR3").index("00").build();
			DBContainer wr3Container = wr3Record.createContainer();
			wr3Container.set("EXCONO", WR0data.get("EXCONO"));
			wr3Container.set("EXFACI", WR0data.get("EXFACI"));
			wr3Container.set("EXHDPR", WR0data.get("EXHDPR"));
			wr3Container.set("EXPRNO", WR0data.get("EXPRNO"));

			if(!wr3Record.readLock(wr3Container){LockedResult updatedRecord ->
						updatedRecord.setInt("EXTSKU", updatedRecord.getInt("EXTSKU") + 1);
						updateTrackingField(updatedRecord, "EX");
						updatedRecord.update();
					}) {

				wr3Container.set("EXORIG", WR0data.get("EXORIG"));
				wr3Container.set("EXNPPN", WR0data.get("EXNPPN"));
				wr3Container.set("EXNPPN", WR0data.get("EXNPPN"));
				wr3Container.set("EXNSPN", WR0data.get("EXNSPN"))
				wr3Container.set("EXNNOT", 0);
				wr3Container.set("EXTSKU", 1)
				wr3Container.set("EXTECT", 0)
				wr3Container.set("EXREST", 0)

				insertTrackingField(wr3Container);

				wr3Record.insert(wr3Container);
			}

			DBAction wr2Record = this.database.table("EXTWR2").index("00").build();
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

		wr0Record.readAll(wr0Container, 2, WR0Closure);
	}

	/**
	 * Calculate value for EXTECT and EXTREST in EXTWR3 
	 * @param cono
	 * @param faci
	 */
	private void calculTectRest(Integer cono, String faci) {
		DBAction wr3Record = database.table("EXTWR3").index("01").selectAllFields().build();
		DBContainer wr3Container = wr3Record.createContainer();
		wr3Container.set("EXCONO", cono);
		wr3Container.set("EXFACI", faci);

		Closure<Boolean> WR3Closure = { DBContainer wr3data ->
			DBAction wr2Record = this.database.table("EXTWR2").index("00").selectAllFields().build();
			DBContainer wr2Container = wr2Record.createContainer();
			wr2Container.set("EXCONO", wr3data.get("EXCONO"));
			wr2Container.set("EXFACI", wr3data.get("EXFACI"));
			wr2Container.set("EXHDPR", wr3data.get("EXHDPR"));

			Double NNOT = 0;
			wr2Record.readLock(wr2Container, {LockedResult updatedRecord ->
				Integer TSKY = updatedRecord.get("EXTSTY");
				Integer NPPN = updatedRecord.get("EXNPPN");
				NNOT = Math.ceil((TSKY/NPPN).toDouble());
				updatedRecord.set("EXNNOT", NNOT);
				updateTrackingField(updatedRecord, "EX");
				updatedRecord.update();
			});

			DBAction wr3RecordUpdate = this.database.table("EXTWR3").index("00").selectAllFields().build();
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

		wr3Record.readAll(wr3Container, 2, WR3Closure);
	}

	/**
	 * Calculate the total number of 'note' 
	 * @param cono
	 * @param faci
	 * @return the total number of 'note' 
	 */
	private int getNbTotNot(Integer cono, String faci) {
		DBAction wr2Record = database.table("EXTWR2").index("00").selection("EXNNOT").build();
		DBContainer wr2Container = wr2Record.createContainer();
		wr2Container.set("EXCONO", cono);
		wr2Container.set("EXFACI", faci);

		int nbTotNot = 0;
		Closure<Integer> WR2Closure = { DBContainer WR2data ->
			nbTotNot += WR2data.getInt("EXNNOT");
		}

		wr2Record.readAll(wr2Container, 2, WR2Closure);
		return nbTotNot
	}

	/**
	 * Fill EXTNOT with usable schn.
	 * @param cono
	 * @param faci
	 * @return true if no error occured.
	 */
	private boolean fillEXNOT(Integer cono, String faci) {

		int nbTotNot = getNbTotNot(cono, faci);
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
						return false;
					}
				});
			}

			DBAction notRecord = this.database.table("EXTNOT").index("00").build();
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

		ExpressionFactory EXTNOTExpressionFactory = database.getExpressionFactory("EXTNOT");
		EXTNOTExpressionFactory =  EXTNOTExpressionFactory.gt("EXREST","0");

		DBAction notRecord = database.table("EXTNOT").index("10").matching(EXTNOTExpressionFactory).selectAllFields().build();
		DBContainer notContainer = notRecord.createContainer();
		notContainer.setInt("EXCONO", cono);


		Closure<Integer> NOTClosure = { DBContainer NOTdata ->
			ExpressionFactory EXTNWR3ExpressionFactory = database.getExpressionFactory(" EXTWR3");
			EXTNWR3ExpressionFactory =  EXTNWR3ExpressionFactory.gt("EXREST","0");

			DBAction wr3Record = database.table("EXTWR3").index("01").matching(EXTNWR3ExpressionFactory).selectAllFields().build();
			DBContainer wr3Container = wr3Record.createContainer();
			wr3Container.set("EXCONO", cono);

			String HDPR;
			int wr3NPPN = 0;

			wr3Record.readAll(wr3Container, 1, 1,{ DBContainer wr3data ->
				HDPR = 	wr3data.getString("EXHDPR");
				wr3NPPN = wr3data.getInt("EXNPPN");
			});

			int aProgNot = NOTdata.getInt("EXREST");
			int pldt = NOTdata.getInt("EXPLDT");

			//L'enregistrement n'a pas encore été utilisé
			if(aProgNot == 9999) {
				aProgNot = wr3NPPN;
				DBAction notURecord = this.database.table("EXTNOT").index("00").build();
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

			EXTNWR3ExpressionFactory =  EXTNWR3ExpressionFactory.and(EXTNWR3ExpressionFactory.eq("EXHDPR",HDPR));
			wr3Record = database.table("EXTWR3").index("01").matching(EXTNWR3ExpressionFactory).selectAllFields().build();

			Closure<Boolean> WR3ReadClosure = { DBContainer wr3data ->
				if(aProgNot > 0) {
					int aProgSku = 0;

					if(wr3data.getInt("EXNSPN") > wr3data.getInt("EXREST"))
						aProgSku = wr3data.getInt("EXREST");
					else
						aProgSku = wr3data.getInt("EXNSPN");

					if(aProgSku >= aProgNot)
						aProgSku = aProgNot;

					if(wr3data.getInt("EXORIG") == 0) {
						aProgSku = 1;
					}

					DBAction wr3URecord = this.database.table("EXTWR3").index("00").build();
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

					ExpressionFactory EXTWR0ExpressionFactory = database.getExpressionFactory("EXTWR0");
					EXTWR0ExpressionFactory =  EXTWR0ExpressionFactory.eq("EXSCHN","0");
					DBAction wr0Record = this.database.table("EXTWR0").index("00").matching(EXTWR0ExpressionFactory).build();
					DBContainer wr0Container = wr0Record.createContainer();
					wr0Container.setInt("EXCONO", wr3data.getInt("EXCONO"));
					wr0Container.setString("EXFACI", wr3data.getString("EXFACI"));
					wr0Container.setString("EXHDPR", wr3data.getString("EXHDPR"));
					wr0Container.setString("EXPRNO", wr3data.getString("EXPRNO"));

					wr0Record.readAll(wr0Container,4, aProgSku, {DBContainer WR0data ->
						DBAction update = this.database.table("EXTWR0").index("00").build();
						DBContainer wr0UpdateContainer = update.createContainer();
						wr0UpdateContainer.set("EXCONO", WR0data.get("EXCONO"));
						wr0UpdateContainer.set("EXFACI", WR0data.get("EXFACI"));
						wr0UpdateContainer.set("EXHDPR", WR0data.get("EXHDPR"));
						wr0UpdateContainer.set("EXPRNO", WR0data.get("EXPRNO"));
						wr0UpdateContainer.set("EXPLPN", WR0data.get("EXPLPN"));
						wr0UpdateContainer.set("EXPLDT", WR0data.get("EXPLDT"));

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
								DBAction notURecord = this.database.table("EXTNOT").index("00").build();
								DBContainer notUContainer = notURecord.createContainer();
								notUContainer.setInt("EXCONO", cono);
								notUContainer.set("EXSCHN", NOTdata.get("EXSCHN"));
								notURecord.readLock(notUContainer,{LockedResult updatedNotRecord ->
									updatedNotRecord.setInt("EXPLDT", pldt);
									updateTrackingField(updatedNotRecord, "EX");
									updatedNotRecord.update();
								});
							}

							Closure<Boolean> PMS170MIUpdatCallback = {Map<String, String> response ->
								if(response.containsKey("error")) {
									miCaller.call("EXT001MI","AddError", ["CONO":cono.toString(),"IFID":"BATCH","FILE":"EXT003", "ERRM":response.errorMessage],{});
									noError = true;
								}
							}

							DBAction moplpRecord = this.database.table("MMOPLP").index("00").build();
							DBContainer moplpContainer = moplpRecord.createContainer();
							moplpContainer.setInt("ROCONO", cono);
							moplpContainer.setInt("ROPLPN", updatedRecord.getInt("EXPLPN"));

							moplpRecord.readLock(moplpContainer,{ LockedResult updtedmoplp ->
								updtedmoplp.set("ROSCHN", NOTdata.get("EXSCHN"));
								updtedmoplp.setInt("ROFIDT", pldt);
								updateTrackingField(updtedmoplp, "RO");
								updtedmoplp.update();
							});
						});
					})
					DBAction notURecord = this.database.table("EXTNOT").index("00").build();
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
			wr3Record.readAll(wr3Container, 1, aProgNot, WR3ReadClosure);
		}

		notRecord.readAll(notContainer, 1, NOTClosure);
		return noError;
	}

	/**
	 * Search for SCHN already used between 88880000000 and 88889999999
	 * @param cono
	 * @param faci
	 * @return a set of used SCHN
	 */
	private Set<Long> usedSCHN(Integer cono, String faci) {
		Set<Long> result = new HashSet<Long>();
		ExpressionFactory mwohedExpressionFactory = this.database.getExpressionFactory("MWOHED");
		mwohedExpressionFactory = mwohedExpressionFactory.ge("VHSCHN", "88880000000").and(mwohedExpressionFactory.le("VHSCHN", "88889999999"));
		DBAction mwohedRecord = this.database.table("MWOHED").index("00").matching(mwohedExpressionFactory).selection("VHSCHN").build();
		DBContainer mwohedContainer = mwohedRecord.createContainer();
		mwohedContainer.setInt("VHCONO", cono);
		mwohedContainer.setString("VHFACI", faci);

		mwohedRecord.readAll(mwohedContainer, 2, { DBContainer MWOHEDdata ->
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
		ExpressionFactory mschmaExpressionFactory = this.database.getExpressionFactory("MSCHMA");
		mschmaExpressionFactory = mschmaExpressionFactory.ge("HSSCHN", "88880000000").and(mschmaExpressionFactory.le("HSSCHN", "88889999999"));
		DBAction mschmaRecord = this.database.table("MSCHMA").index("00").matching(mschmaExpressionFactory).selection("HSSCHN").build();
		DBContainer mschmaContainer = mschmaRecord.createContainer();
		mschmaContainer.setInt("HSCONO", cono);

		mschmaRecord.readAll(mschmaContainer, 1, { DBContainer MSCHMAdata ->
			long schn = MSCHMAdata.getLong("HSSCHN");
			if(usedSCHN.contains(schn)) {
				result.add(schn);
			}
		});
		return result;
	}

	/**
	 * Calculate the priority of the fabrication orders.
	 * @param cono
	 * @param faci
	 */
	private void initOrig(int cono, String faci) {
		collectOrig(cono, faci);
		qualifOF(cono, faci);
	}

	/**
	 * Calculate needs by priority.
	 * @param cono
	 * @param faci
	 */
	private void collectOrig(int cono, String faci) {
		ExpressionFactory mitploExpressionFactory = this.database.getExpressionFactory("MITPLO");
		mitploExpressionFactory = mitploExpressionFactory.ge("MOTRQT", "0");

		DBAction mitploRecord = this.database.table("MITPLO").index("20").matching(mitploExpressionFactory).selection("MOITNO","MOTRQT","MOORCA","MOTRTP").build();
		DBContainer mitploContainer = mitploRecord.createContainer();
		mitploContainer.setInt("MOCONO", cono);

		mitploRecord.readAll(mitploContainer, 1,{ DBContainer MITPLOdata ->
			DBAction mitmasRecord = this.database.table("MITMAS").index("00").selection("MMCHCD").build();
			DBContainer mitmasContainer = mitmasRecord.createContainer();
			mitmasContainer.setInt("MMCONO", cono);
			mitmasContainer.setString("MMITNO", MITPLOdata.getString("MOITNO"));
			mitmasRecord.read(mitmasContainer);
			if(mitmasContainer.getInt("MMCHCD")==3) {
				int origBesoin = getOrigineBesoin(cono, MITPLOdata.getString("MOORCA"), MITPLOdata.getString("MOTRTP"));
				addTrqtExtbes("EXTRQ"+origBesoin, cono, faci, MITPLOdata.getString("MOITNO"), MITPLOdata.getDouble("MOTRQT"));
			}
		});
	}

	/**
	 * get origin need by order category or order category and type
	 * @param cono Compagny number
	 * @param orca order category
	 * @param trtp order type
	 * @return origin number
	 */
	private int getOrigineBesoin(int cono, String orca, String trtp) {
		DBAction cugex1Record = this.database.table("CUGEX1").index("00").selection("F1N096").build();
		DBContainer cugex1Container = cugex1Record.createContainer();
		cugex1Container.setInt("F1CONO", cono);
		cugex1Container.setString("F1FILE", "EXTEND");
		cugex1Container.setString("F1PK01","ORIGINE");
		cugex1Container.setString("F1PK02", orca);

		if(cugex1Record.read(cugex1Container)) {
			return cugex1Container.getDouble("N096").toInteger();
		}else {
			cugex1Container.setString("F1PK03", trtp);
			if(cugex1Record.read(cugex1Container)) {
				return cugex1Container.getDouble("N096").toInteger();
			}
		}
		return 9;
	}

	/**
	 * Add transaction quantity in table extbes
	 * @param field Field to add quantity
	 * @param cono compagny
	 * @param faci 
	 * @param prno SKU
	 * @param trqt Quanity to add
	 */
	private void addTrqtExtbes(String field, int cono, String faci, String prno, double trqt ) {
		DBAction extbesRecord = this.database.table("EXTBES").index("00").build();
		DBContainer extbesContainer = extbesRecord.createContainer();
		extbesContainer.setInt("EXCONO", cono);
		extbesContainer.setString("EXFACI", faci);
		extbesContainer.setString("EXPRNO", prno);

		if(!extbesRecord.readLock(extbesContainer, { LockedResult updateRecoord ->
					updateRecoord.setDouble("EXTRQ1", updateRecoord.getDouble("EXTRQ1") + trqt);
					updateTrackingField(updateRecoord, "EX");
					updateRecoord.update();
				})) {
			extbesContainer.setDouble("EXTRQ1", trqt);
			insertTrackingField(extbesContainer);
			extbesRecord.insert(extbesContainer);
		}

	}

	/**
	 * Set prioruty on new fabrication orders.
	 * @param cono
	 * @param faci
	 */
	private void qualifOF(int cono, String faci) {
		DBAction extbesRecord = this.database.table("EXTBES").index("00").selectAllFields().build();
		DBContainer extbesContainer = extbesRecord.createContainer();
		extbesContainer.setInt("EXCONO", cono);
		extbesContainer.setString("EXFACI", faci);

		extbesRecord.readAllLock(extbesContainer, 2,{  LockedResult EXTBESupdate ->
			ExpressionFactory mwohedExpressionFactory = this.database.getExpressionFactory("MITPLO");
			mwohedExpressionFactory = mwohedExpressionFactory.lt("VHWHST", "90");

			DBAction mwohedRecord = this.database.table("MWOHED").index("00").matching(mwohedExpressionFactory).selection("VHSCHN", "VHMAQT", "VHORQT", "VHPRNO").build();
			DBContainer mwohedContainer = mwohedRecord.createContainer();
			mwohedContainer.setInt("VHCONO", cono);
			mwohedContainer.setString("VHFACI", faci);
			mwohedContainer.setString("VHPRNO", EXTBESupdate.getString("EXPRNO"));

			mwohedRecord.readAllLock(mwohedContainer, 3, {  LockedResult MWOHEDupdate ->
				if(MWOHEDupdate.getDouble("VHMAQT") >= MWOHEDupdate.getDouble("VHORQT"))
					return;
				String schn = ""+MWOHEDupdate.getLong("VHSCHN");
				if(schn != MWOHEDupdate.getString("VHMFNO")) {
					boolean updated = false;
					if(EXTBESupdate.getDouble("EXTRQ0")> 0) {
						EXTBESupdate.setDouble("EXTRQ0", EXTBESupdate.getDouble("EXTRQ0") - MWOHEDupdate.getDouble("VHORQT"));
						MWOHEDupdate.setInt("VHPRIO", 0);
						updated = true;
					}else if(EXTBESupdate.getDouble("EXTRQ1")> 0) {
						EXTBESupdate.setDouble("EXTRQ1", EXTBESupdate.getDouble("EXTRQ1") - MWOHEDupdate.getDouble("VHORQT"));
						MWOHEDupdate.setInt("VHPRIO", 1);
						updated = true;
					}else if(EXTBESupdate.getDouble("EXTRQ2")> 0) {
						EXTBESupdate.setDouble("EXTRQ2", EXTBESupdate.getDouble("EXTRQ2") - MWOHEDupdate.getDouble("VHORQT"));
						MWOHEDupdate.setInt("VHPRIO", 2);
						updated = true;
					}else if(EXTBESupdate.getDouble("EXTRQ3")> 0) {
						EXTBESupdate.setDouble("EXTRQ3", EXTBESupdate.getDouble("EXTRQ3") - MWOHEDupdate.getDouble("VHORQT"));
						MWOHEDupdate.setInt("VHPRIO", 3);
						updated = true;
					}else if(EXTBESupdate.getDouble("EXTRQ4")> 0) {
						EXTBESupdate.setDouble("EXTRQ4", EXTBESupdate.getDouble("EXTRQ4") - MWOHEDupdate.getDouble("VHORQT"));
						MWOHEDupdate.setInt("VHPRIO", 4);
						updated = true;
					}else if(EXTBESupdate.getDouble("EXTRQ5")> 0) {
						EXTBESupdate.setDouble("EXTRQ5", EXTBESupdate.getDouble("EXTRQ5") - MWOHEDupdate.getDouble("VHORQT"));
						MWOHEDupdate.setInt("VHPRIO", 5);
						updated = true;
					}else if(EXTBESupdate.getDouble("EXTRQ6")> 0) {
						EXTBESupdate.setDouble("EXTRQ6", EXTBESupdate.getDouble("EXTRQ6") - MWOHEDupdate.getDouble("VHORQT"));
						MWOHEDupdate.setInt("VHPRIO", 6);
						updated = true;
					}else if(EXTBESupdate.getDouble("EXTRQ7")> 0) {
						EXTBESupdate.setDouble("EXTRQ7", EXTBESupdate.getDouble("EXTRQ7") - MWOHEDupdate.getDouble("VHORQT"));
						MWOHEDupdate.setInt("VHPRIO", 7);
						updated = true;
					}else if(EXTBESupdate.getDouble("EXTRQ8")> 0) {
						EXTBESupdate.setDouble("EXTRQ8", EXTBESupdate.getDouble("EXTRQ8") - MWOHEDupdate.getDouble("VHORQT"));
						MWOHEDupdate.setInt("VHPRIO", 8);
						updated = true;
					}else if(EXTBESupdate.getDouble("EXTRQ9")> 0) {
						EXTBESupdate.setDouble("EXTRQ9", EXTBESupdate.getDouble("EXTRQ9") - MWOHEDupdate.getDouble("VHORQT"));
						MWOHEDupdate.setInt("VHPRIO", 9);
						updated = true;
					}

					if(updated) {
						updateTrackingField(EXTBESupdate, "EX");
						EXTBESupdate.update();
						updateTrackingField(MWOHEDupdate, "VH");
						MWOHEDupdate.update();
					}
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
		DBAction extbesRecord = this.database.table("EXTBES").index("00").selectAllFields().build();
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