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

	private String CUS_PLGR = "E_CPDES";
	private String CUS_EMPL_MORCEAUX = "PEAUSSERIE";
	private String CUS_MORCEAU = "ZZ1";
	private int CUS_CSEQ = 7778;
	private int CUS_MSEQ = 8889;

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
		if(!mitmasRecord.read(mitmasContainer) || mitmasContainer.getString("MMGRTI").equals(CUS_MORCEAU)) {
			mi.error(MTNO + " n'existe pas ou n'est pas un morceau");
			return;
		}

		ExpressionFactory mwoopeExpressionFactory =  database.getExpressionFactory("MWOOPE");
		mwoopeExpressionFactory = mwoopeExpressionFactory.eq("VOPLGR", PLGR);
		DBAction mwoopeRecord = database.table("MWOOPE").index("90").matching(mwoopeExpressionFactory).selection("VOPRNO", "VOMFNO", "VOOPNO").build();
		DBContainer mwoopeContainer = mwoopeRecord.createContainer();
		mwoopeContainer.setInt("VOCONO", CONO);
		mwoopeContainer.setString("VOFACI", FACI);
		mwoopeContainer.setLong("VOSCHN", DEBI);

		int nbOF = 0;
		mwoopeRecord.readAll(mwoopeContainer, 3, { DBContainer mwoopeData ->
			nbOF++;
		});

		if(nbOF == 0) {
			nbOF = 1;
		}

		double calcRpqt = RPQT / nbOF;

		mwoopeRecord.readAll(mwoopeContainer, 3, { DBContainer mwoopeData ->
			int mseq = getMseq(CONO, FACI, CUS_MSEQ, mwoopeData);
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

	private void insertTrackingField(DBContainer insertedRecord, String prefix) {
		insertedRecord.set(prefix+"RGDT", (Integer) this.utility.call("DateUtil", "currentDateY8AsInt"));
		insertedRecord.set(prefix+"LMDT", (Integer) this.utility.call("DateUtil", "currentDateY8AsInt"));
		insertedRecord.set(prefix+"CHID", this.program.getUser());
		insertedRecord.set(prefix+"RGTM", (Integer) this.utility.call("DateUtil", "currentTimeAsInt"));
		insertedRecord.set(prefix+"CHNO", 1);
	}


	private boolean checkInputs(Integer cono, String  faci, String plgr, Long debi, String mtno, Double rpqt) {
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

	private void init(int cono) {
		DBAction CUGEX1Record = database.table("CUGEX1").index("00").selection("F1A030","F1N096","F1N196","F1CHB1").build();
		DBContainer CUGEX1Container = CUGEX1Record.createContainer();
		CUGEX1Container.setInt("F1CONO", cono);
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

	}

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