/**
 * README
 *
 * Name: EXT005MI.AddMorceau
 * Description: Ajouter un morceau
 * Date                         Changed By                         Description
 * 20240227                     ddecosterd@hetic3.fr     	création
 */
public class AddMorceau extends ExtendM3Transaction {
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

	public AddMorceau(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility, MICallerAPI miCaller) {
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
		String MTNO = (mi.inData.get("MTNO") == null) ? "" : mi.inData.get("MTNO").trim();
		Double RPQT = mi.in.get("RPQT");

		if(!checkInputs(CONO, FACI, PLGR, DEBI, MTNO, RPQT))
			return;

		init(CONO);

		DBAction mitmasRecord = database.table("MITMAS").selection("MMGRTI").build();
		DBContainer mitmasContainer = mitmasRecord.createContainer();
		mitmasContainer.setInt("MMCONO", CONO);
		mitmasContainer.setString("MMITNO", MTNO);
		if(!mitmasRecord.read(mitmasContainer) || mitmasContainer.getString("MMGRTI").equals(cusMorceau)) {
			mi.error(MTNO + " n'existe pas ou n'est pas un morceau");
			return;
		}

		DBAction mwoopeRecord = database.table("MWOOPE").index("70").selection("VOPRNO", "VOMFNO", "VOOPNO").build();
		DBContainer mwoopeContainer = mwoopeRecord.createContainer();
		mwoopeContainer.setInt("VOCONO", CONO);
		mwoopeContainer.setString("VOFACI", FACI);
		mwoopeContainer.setString("VOPLGR", PLGR);
		mwoopeContainer.setLong("VOSCHN", DEBI);

		int nbOF = 0;
		mwoopeRecord.readAll(mwoopeContainer, 4, 1000, { DBContainer mwoopeData ->
			nbOF++;
		});

		if(nbOF == 0) {
			nbOF = 1;
		}

		double calcRpqt = RPQT / nbOF;

		mwoopeRecord.readAll(mwoopeContainer, 4, 1000, { DBContainer mwoopeData ->
			int mseq = getMseq(CONO, FACI, cusMseq, mwoopeData);
			mseq--;

			Map<String,String> pms100miParameters =  [CONO:CONO.toString(),FACI:FACI,PRNO:mwoopeData.getString("VOPRNO"),
				OPNO:mwoopeData.getInt("VOOPNO").toString(),MSEQ:mseq.toString(),MTNO:MTNO,MFNO:mwoopeData.getString("VOMFNO"),
				CNQT:calcRpqt.round(5).toString(),FXCD:"1",BYPR:"1"];

			miCaller.call("PMS100MI", "AddMOComponent", pms100miParameters , { Map<String, String> response ->
				if(response.containsKey("error")) {
					mi.error(response.errorMessage);
				}
			});
		});

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
	 * Check input values
	 * @param cono
	 * @param faci
	 * @param plgr
	 * @param debi
	 * @param mtno
	 * @param rpqt
	 * @return true if no error.
	 */
	private boolean checkInputs(Integer cono, String  faci, String plgr, Long debi, String mtno, Double rpqt) {
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

		if(mtno == null || mtno.isEmpty()) {
			mi.error("Numéro de composant est obligatoire.");
			return false;
		}

		if(rpqt == null) {
			mi.error("le champ RPQT est obligatoire.");
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
	 * Get an mseq between 7000 and maxMseq
	 * @param cono
	 * @param faci
	 * @param maxMseq
	 * @param mwoopeData
	 * @return
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
}