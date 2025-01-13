/**
 * README
 *
 * Name: EXT005MI.CtlDebit
 * Description: Contrôle la note de débit
 * Date                         Changed By                         Description
 * 20240223                     ddecosterd@hetic3.fr     	création
 */
public class CtlDebit extends ExtendM3Transaction {
	private final MIAPI mi;
	private final DatabaseAPI database;
	private final UtilityAPI utility;

	public CtlDebit(MIAPI mi, DatabaseAPI database, UtilityAPI utility) {
		this.mi = mi;
		this.database = database;
		this.utility = utility;
	}


	public void main() {
		Integer CONO = mi.in.get("CONO");
		String  FACI = (mi.inData.get("FACI") == null) ? "" : mi.inData.get("FACI").trim();
		String PLGR = (mi.inData.get("PLGR") == null) ? "" : mi.inData.get("PLGR").trim();
		Integer OPNO = mi.in.get("OPNO");
		Long DEBI = mi.in.get("DEBI");

		if(!checkInputs(CONO, FACI, PLGR, OPNO, DEBI))
			return;

		ExpressionFactory mwoopeExpressionFactory = database.getExpressionFactory("MWOOPE");
		mwoopeExpressionFactory =  mwoopeExpressionFactory.eq("VOOPNO",OPNO.toString());
		DBAction mwoopeRecord = database.table("MWOOPE").index("70").matching(mwoopeExpressionFactory).selection("VOPRNO").build();
		DBContainer mwoopeContainer = mwoopeRecord.createContainer();
		mwoopeContainer.setInt("VOCONO", CONO);
		mwoopeContainer.setString("VOFACI", FACI);
		mwoopeContainer.setString("VOPLGR", PLGR);
		mwoopeContainer.setLong("VOSCHN", DEBI);

		Closure<?> mwoopeClosure = { DBContainer mwoopeData ->
			DBAction mitmasRecord = database.table("MITMAS").index("00").build();
			DBContainer mitmasContainer = mitmasRecord.createContainer();
			mitmasContainer.setInt("MMCONO", CONO);
			mitmasContainer.setString("MMITNO", mwoopeData.getString("VOPRNO"));
			if(mitmasRecord.read(mitmasContainer)) {
				mi.getOutData().put("MODR", "D");
				mi.write();
			}
		};

		if(mwoopeRecord.readAll(mwoopeContainer, 4, 1, mwoopeClosure) == 0) {
			mi.error("ce numéro de fiche de débit est invalide");
		}
	}

	/**
	 * Check input values
	 * @param cono
	 * @param faci
	 * @param plgr
	 * @param opno
	 * @param debi
	 * @return true if no error.
	 */
	private boolean checkInputs(Integer cono, String  faci, String plgr, Integer opno, Long debi) {
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

		if(debi == null) {
			mi.error("La note de débit est obligatoire.");
			return false;
		}
		if(!utility.call("CheckUtil", "checkSCHNExist", database, cono, debi)) {
			mi.error("La note de débit est inexistante");
			return false;
		}

		return true;
	}
}