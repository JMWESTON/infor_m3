/**
 * README
 *
 * Name: EXT005MI.LstMere
 * Description: Liste les opérations reliées à la note mère
 * Date                         Changed By                         Description
 * 20240222                     ddecosterd@hetic3.fr     	création
 */
import java.sql.DatabaseMetaData

public class LstMere extends ExtendM3Transaction {
	private final MIAPI mi;
	private final DatabaseAPI database;
	private final UtilityAPI utility;

	public LstMere(MIAPI mi, DatabaseAPI database, UtilityAPI utility) {
		this.mi = mi;
		this.database = database;
		this.utility = utility;
	}

	public void main() {
		Integer CONO = mi.in.get("CONO");
		String  FACI = (mi.inData.get("FACI") == null) ? "" : mi.inData.get("FACI").trim();
		String PLGR = (mi.inData.get("PLGR") == null) ? "" : mi.inData.get("PLGR").trim();
		Integer OPNO = mi.in.get("OPNO");
		Long MERE = mi.in.get("MERE");
		Long FMER = mi.in.get("FMER");

		if(!checkInputs(CONO, FACI, PLGR, OPNO, MERE, FMER))
			return;

		ExpressionFactory MWOOPEExpressionFactory = database.getExpressionFactory("MWOOPE");
		MWOOPEExpressionFactory = MWOOPEExpressionFactory.eq("VOOPNO", OPNO.toString());
		DBAction mwoopeRecord = database.table("MWOOPE").index("70").matching(MWOOPEExpressionFactory).selection("VOPRNO", "VOMFNO", "VOOPNO", "VOORQA").build();
		DBContainer mwoopeContainer = mwoopeRecord.createContainer();
		mwoopeContainer.setInt("VOCONO", CONO);
		mwoopeContainer.setString("VOFACI", FACI);
		mwoopeContainer.setString("VOPLGR", PLGR);
		mwoopeContainer.setLong("VOSCHN", MERE);

		mwoopeRecord.readAll(mwoopeContainer, 4, 1000, { DBContainer mwoopeData ->
			boolean getLine = true;
			if(FMER!= null && FMER>0) {
				DBAction mwohedRecord = database.table("MWOHED").index("00").selection("VHSCHN").build();
				DBContainer mwohedContainer = mwohedRecord.createContainer();
				mwohedContainer.setInt("VHCONO", CONO);
				mwohedContainer.setString("VHFACI", FACI);
				mwohedContainer.setString("VHPRNO", mwoopeData.getString("VOPRNO"));
				mwohedContainer.setString("VHMFNO", mwoopeData.getString("VOMFNO"));

				mwohedRecord.read(mwohedContainer);
				if(mwohedContainer.getLong("VHSCHN") != FMER) {
					getLine = false;
				}

			}
			if(getLine) {
				mi.getOutData().put("FACI", FACI);
				mi.getOutData().put("PRNO", mwoopeData.get("VOPRNO").toString());
				mi.getOutData().put("MFNO", mwoopeData.get("VOMFNO").toString());
				mi.getOutData().put("OPNO", mwoopeData.get("VOOPNO").toString());
				mi.getOutData().put("ORQA", mwoopeData.get("VOORQA").toString());
				mi.write();
			}
		});
	}

	/**
	 * Check input values
	 * @param cono
	 * @param faci
	 * @param plgr
	 * @param opno
	 * @param mere
	 * @param fmer
	 * @return true if no error.
	 */
	private boolean checkInputs(Integer cono, String  faci, String plgr, Integer opno, Long mere, Long fmer) {
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

		if(opno == null) {
			mi.error("Le numéro d'opération est obligatoire.");
			return false;
		}

		if(mere == null) {
			mi.error("La note de débit est obligatoire.");
			return false;
		}
		if(!utility.call("CheckUtil", "checkSCHNExist", database, cono, mere)) {
			mi.error("La note de débit est inexistante");
			return false;
		}


		if(fmer != null && !utility.call("CheckUtil", "checkSCHNExist", database, cono, fmer)) {
			mi.error("La note de débit est inexistante");
			return false;
		}

		return true;
	}
}