/**
 * README
 *
 * Name: EXT005MI.UpMatLine
 * Description: Remplacement de composant
 * Date                         Changed By                         Description
 * 20240222                     ddecosterd@hetic3.fr     	création
 */
public class UpdMatLine extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;
	private final MICallerAPI miCaller;

	private boolean hadError= false;
	private String errorMessage;

	private String cusPlgr = "E_CPDES";
	private String cusEmplMorceaux = "PEAUSSERIE";
	private String cusMorceau = "ZZ1";
	private int cusCseq = 7778;
	private int cusMseq = 8889;

	public UpdMatLine(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility, MICallerAPI miCaller) {
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
		Integer OPNO = mi.in.get("OPNO");
		Long DEBI = mi.in.get("DEBI");
		String MFNO = (mi.inData.get("MFNO") == null) ? "" : mi.inData.get("MFNO").trim();
		String MODR = (mi.inData.get("MODR") == null) ? "" : mi.inData.get("MODR").trim();
		String MTNO = (mi.inData.get("MTNO") == null) ? "" : mi.inData.get("MTNO").trim();
		String MTNR = (mi.inData.get("MTNR") == null) ? "" : mi.inData.get("MTNR").trim();

		if(!checkInputs(CONO, FACI, PLGR, OPNO, DEBI, MFNO, MODR, MTNO, MTNR))
			return;

		init(CONO);

		if(MODR == "D") {
			DBAction mwoopeRecord;
			DBContainer mwoopeContainer;
			mwoopeRecord = database.table("MWOOPE").index("70").build();
			mwoopeContainer = mwoopeRecord.createContainer();
			mwoopeContainer.setInt("VOCONO", CONO);
			mwoopeContainer.setString("VOFACI", FACI);
			mwoopeContainer.setString("VOPLGR", PLGR);
			mwoopeContainer.setLong("VOSCHN", DEBI);
			mwoopeRecord.readAll(mwoopeContainer, 4, 1000,{ DBContainer mwoopeData ->
				mwoopeCallback(mwoopeData, CONO, FACI, MTNO, MTNR);
			});
		}else
			if(MODR == "O") {
				DBAction mwohedRecord = database.table("MWOHED").index("55").build();
				DBContainer mwohedContainer = mwohedRecord.createContainer();
				mwohedContainer.setInt("VHCONO", CONO);
				mwohedContainer.setString("VHFACI", FACI);
				mwohedContainer.setString("VHMFNO", MFNO);
				mwohedRecord.readAll(mwohedContainer, 3, 1,{ DBContainer mwohedData ->
					DBAction mwoope00Record = database.table("MWOOPE").index("00").selection("VOPLGR","VOSCHN").build();
					DBContainer mwoope00Container = mwoope00Record.createContainer();
					mwoope00Container.setInt("VOCONO", CONO);
					mwoope00Container.setString("VOFACI", FACI);
					mwoope00Container.setString("VOPRNO", mwohedData.getString("VHPRNO"));
					mwoope00Container.setString("VOMFNO", MFNO);
					mwoope00Container.setInt("VOOPNO", OPNO);
					mwoope00Record.readAll(mwoope00Container, 5,1,{ DBContainer mwoopeData ->
						mwoopeCallback(mwoopeData, CONO, FACI, MTNO, MTNR);
					});
				});
			}

		if(hadError) {
			mi.error(errorMessage);
		}
	}

	/**
	 *  Add default value for new record.
	 * @param insertedRecord
	 * @param prefix The column prefix of the table.
	 */
	private void insertTrackingField(DBContainer insertedRecord, String prefix) {
		// Set the change tracking fields
		insertedRecord.set(prefix+"RGDT", (Integer) utility.call("DateUtil", "currentDateY8AsInt"));
		insertedRecord.set(prefix+"LMDT", (Integer) utility.call("DateUtil", "currentDateY8AsInt"));
		insertedRecord.set(prefix+"CHID", program.getUser());
		insertedRecord.set(prefix+"RGTM", (Integer) utility.call("DateUtil", "currentTimeAsInt"));
		insertedRecord.set(prefix+"CHNO", 1);
	}


	/**
	 * Check input values
	 * @param cono
	 * @param faci
	 * @param plgr
	 * @param opno
	 * @param debi
	 * @param mfno
	 * @param modr
	 * @param mtno
	 * @param mtnr
	 * @return true if no error.
	 */
	private boolean checkInputs(Integer cono, String  faci, String plgr, Integer opno, Long debi, String mfno, String modr, String mtno, String mtnr) {
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

		if(opno == null || opno == 0) {
			mi.error("Le numéro d'opération est obligatoire.");
			return false;
		}

		if(modr == null) {
			mi.error("Mode de remplacement est obligatoire.");
			return false;
		}
		if( !modr.equals("D") && !modr.equals("O")) {
			mi.error("Mode de remplacement incorrect.");
			return false;
		}

		if(modr.equals("D")) {
			if(debi == null) {
				mi.error("La note de débit est obligatoire quand mode de remplacement est D.");
				return false;
			}
			if(!utility.call("CheckUtil", "checkSCHNExist", database, cono, debi)) {
				mi.error("La note de débit est inexistante.");
				return false;
			}
		}

		if(modr.equals("O") && (mfno == null || mfno.isEmpty())) {
			mi.error("Numéro d4OF est obligatoire quand mode de remplacement est O.");
			return false;
		}

		if(mtno == null || mtno.isEmpty()) {
			mi.error("Composant à remplacer est obligatoire.");
			return false;
		}

		if(mtnr == null || mtnr.isEmpty()) {
			mi.error("Composant remplaçant est obligatoire.");
			return false;
		}

		return true;
	}

	/**
	 * Get config values
	 * @param cono
	 */
	private void init(int cono) {
		DBAction CUGEX1Record = database.table("CUGEX1").index("00").selection("F1A030","F1N096","F1N196","F1CHB1").build();
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
	 * Search for the last MSEQ used
	 * @param cono
	 * @param faci
	 * @param maxMseq
	 * @param mwoopeData
	 * @return the MSEQ
	 */
	private int getMseq(int cono, String faci, int maxMseq, DBContainer mwoopeData) {
		int mseq = maxMseq;
		ExpressionFactory  mwomat10ExpressionFactory = database.getExpressionFactory(" MWOMAT");
		mwomat10ExpressionFactory =   mwomat10ExpressionFactory.between("VMMSEQ","7000",""+maxMseq);
		DBAction mwomat10Record = database.table("MWOMAT").index("10").matching(mwomat10ExpressionFactory).selection("VMMSEQ").build();
		DBContainer mwomat10Container = mwomat10Record.createContainer();
		mwomat10Container.setInt("VMCONO", cono);
		mwomat10Container.setString("VMFACI", faci);
		mwomat10Container.setString("VMPRNO",mwoopeData.getString("VOPRNO"));
		mwomat10Container.setString("VMMFNO", mwoopeData.getString("VOMFNO"));
		mwomat10Container.setInt("VMOPNO", mwoopeData.getInt("VOOPNO"));

		mwomat10Record.readAll(mwomat10Container, 5, 1, { DBContainer mwomatData ->
			mseq = mwomatData.getInt("VMMSEQ");
		});

		return mseq;
	}

	/**
	 * remplace le composant par le nouveau
	 * @param mwomatData
	 * @param mtnr
	 */
	private void remplaceComposant(DBContainer mwomatData, String mtnr) {
		Map<String,String> parameters =  ["CONO":mwomatData.getInt("VMCONO").toString(),
			FACI:mwomatData.getString("VMFACI"),PRNO:mwomatData.getString("VMPRNO"),MFNO:mwomatData.getString("VMMFNO"),
			OPNO:mwomatData.getInt("VMOPNO").toString(),MSEQ:mwomatData.getInt("VMMSEQ").toString(),MTNO:mtnr];

		miCaller.call("PMS100MI", "UpdMatLine", parameters , { Map<String, String> response ->
			if(response.error) {
				errorMessage = response.errorMessage;
				hadError = true;
			}
		});

	}

	/**
	 * Ajoute le nouveau composant
	 * @param mwomatData
	 * @param mtnr
	 * @param mseq
	 */
	private void ajoutComposant(DBContainer mwomatData, String mtnr, int mseq) {
		Map<String,String> parameters =  ["CONO":mwomatData.getInt("VMCONO").toString(),
			FACI:mwomatData.getString("VMFACI"),PRNO:mwomatData.getString("VMPRNO"),MFNO:mwomatData.getString("VMMFNO"),
			OPNO:mwomatData.getInt("VMOPNO").toString(),MSEQ:mseq.toString(),MTNO:mtnr,CNQT:"0.000001"];

		miCaller.call("PMS100MI", "AddMOComponent", parameters , { Map<String, String> response ->
			if(response.error) {
				errorMessage = response.errorMessage;
				hadError = true;
			}
		});
	}

	/**
	 * Traitement de la ligne de MWOOPE
	 * @param mwoopeData
	 * @param cono
	 * @param faci
	 * @param mtno
	 * @param mtnr
	 */
	private void mwoopeCallback(DBContainer mwoopeData, int cono, String faci, String mtno, String mtnr) {
		if(!hadError) {
			boolean wwAjout = false;
			boolean wwAjoute = false;

			int wwMseq = getMseq(cono, faci, cusCseq, mwoopeData);
			wwMseq--;

			ExpressionFactory mwomatExpressionFactory = database.getExpressionFactory("MWOMAT");
			mwomatExpressionFactory = mwomatExpressionFactory.eq("VMMTNO", mtno);
			DBAction mwomat12Record = database.table("MWOMAT").index("12").matching(mwomatExpressionFactory).selection("VMRPQT").reverse().build();
			DBContainer mwomat12Container = mwomat12Record.createContainer();
			mwomat12Container.setInt("VMCONO", cono);
			mwomat12Container.setString("VMFACI", faci);
			mwomat12Container.setString("VMPRNO",mwoopeData.getString("VOPRNO"));
			mwomat12Container.setString("VMMFNO", mwoopeData.getString("VOMFNO"));
			mwomat12Container.setInt("VMOPNO", mwoopeData.getInt("VOOPNO"));

			mwomat12Record.readAll(mwomat12Container, 5, 30, { DBContainer mwomatData ->
				if(mwomatData.getDouble("VMRPQT") != 0) {
					wwAjout = true;
				}
				if(wwAjout) {
					if(!wwAjoute) {
						ajoutComposant(mwomatData, mtnr, wwMseq);
						wwAjoute = true;
					}
				}else {
					remplaceComposant(mwomatData, mtnr);
				}
			});
		}


	}
}