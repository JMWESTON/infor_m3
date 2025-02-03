
/**
 * README
 *
 * Name: EXT007MI.AddNeed
 * Description: Add need to table EXTBES
 * Date                         Changed By                    Description
 * 20250131                     ddecosterd@hetic3.fr     		création
 */
public class AddNeed extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;

	public AddNeed(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility) {
		this.mi = mi;
		this.program = program;
		this.database = database;
		this.utility = utility;
	}

	public void main() {
		Integer CONO = mi.in.get("CONO");
		String  FACI = (mi.inData.get("FACI") == null) ? "" : mi.inData.get("FACI").trim();
		String  PRNO = (mi.inData.get("PRNO") == null) ? "" : mi.inData.get("PRNO").trim();
		Double TRQ0 = mi.in.get("TRQ0");
		Double TRQ1 = mi.in.get("TRQ1");
		Double TRQ2 = mi.in.get("TRQ2");
		Double TRQ3 = mi.in.get("TRQ3");
		Double TRQ4 = mi.in.get("TRQ4");
		Double TRQ5 = mi.in.get("TRQ5");
		Double TRQ6 = mi.in.get("TRQ6");
		Double TRQ7 = mi.in.get("TRQ7");
		Double TRQ8 = mi.in.get("TRQ8");
		Double TRQ9 = mi.in.get("TRQ9");

		if(TRQ0 == null) TRQ0 = 0;
		if(TRQ1 == null) TRQ1 = 0;
		if(TRQ2 == null) TRQ2 = 0;
		if(TRQ3 == null) TRQ3 = 0;
		if(TRQ4 == null) TRQ4 = 0;
		if(TRQ5 == null) TRQ5 = 0;
		if(TRQ6 == null) TRQ6 = 0;
		if(TRQ7 == null) TRQ7 = 0;
		if(TRQ8 == null) TRQ8 = 0;
		if(TRQ9 == null) TRQ9 = 0;

		if(!checkInputs(CONO, FACI, PRNO, TRQ0, TRQ1, TRQ2, TRQ3, TRQ4, TRQ5, TRQ6, TRQ7, TRQ8, TRQ9)) {
			return;
		}

		DBAction extbesRecord = database.table("EXTBES").index("00").build();
		DBContainer extbesContainer = extbesRecord.createContainer();
		extbesContainer.setInt("EXCONO", CONO);
		extbesContainer.setString("EXFACI", FACI);
		extbesContainer.setString("EXPRNO", PRNO);

		if(!extbesRecord.readLock(extbesContainer, { LockedResult updateRecoord ->
					updateRecoord.setDouble("EXTRQ0", updateRecoord.getDouble("EXTRQ0") - TRQ0);
					updateRecoord.setDouble("EXTRQ1", updateRecoord.getDouble("EXTRQ1") - TRQ1);
					updateRecoord.setDouble("EXTRQ2", updateRecoord.getDouble("EXTRQ2") - TRQ2);
					updateRecoord.setDouble("EXTRQ3", updateRecoord.getDouble("EXTRQ3") - TRQ3);
					updateRecoord.setDouble("EXTRQ4", updateRecoord.getDouble("EXTRQ4") - TRQ4);
					updateRecoord.setDouble("EXTRQ5", updateRecoord.getDouble("EXTRQ5") - TRQ5);
					updateRecoord.setDouble("EXTRQ6", updateRecoord.getDouble("EXTRQ6") - TRQ6);
					updateRecoord.setDouble("EXTRQ7", updateRecoord.getDouble("EXTRQ7") - TRQ7);
					updateRecoord.setDouble("EXTRQ8", updateRecoord.getDouble("EXTRQ8") - TRQ8);
					updateRecoord.setDouble("EXTRQ9", updateRecoord.getDouble("EXTRQ9") - TRQ9);
					int CHNO = updateRecoord.getInt("EXCHNO");
					if(CHNO== 999) {CHNO = 0;}
					updateRecoord.set("EXLMDT", (Integer) utility.call("DateUtil", "currentDateY8AsInt"));
					updateRecoord.set("EXCHID", program.getUser());
					updateRecoord.setInt("EXCHNO", CHNO);
					updateRecoord.update();
				})) {
			extbesContainer.setDouble("EXTRQ0", -TRQ0);
			extbesContainer.setDouble("EXTRQ1", -TRQ1);
			extbesContainer.setDouble("EXTRQ2", -TRQ2);
			extbesContainer.setDouble("EXTRQ3", -TRQ3);
			extbesContainer.setDouble("EXTRQ4", -TRQ4);
			extbesContainer.setDouble("EXTRQ5", -TRQ5);
			extbesContainer.setDouble("EXTRQ6", -TRQ6);
			extbesContainer.setDouble("EXTRQ7", -TRQ7);
			extbesContainer.setDouble("EXTRQ8", -TRQ8);
			extbesContainer.setDouble("EXTRQ9", -TRQ9);
			extbesContainer.set("EXRGDT", (Integer) utility.call("DateUtil", "currentDateY8AsInt"));
			extbesContainer.set("EXLMDT", (Integer) utility.call("DateUtil", "currentDateY8AsInt"));
			extbesContainer.set("EXCHID", program.getUser());
			extbesContainer.set("EXRGTM", (Integer) utility.call("DateUtil", "currentTimeAsInt"));
			extbesContainer.set("EXCHNO", 1);
			extbesRecord.insert(extbesContainer);
		}

	}

	/**
	 * Check input values
	 * @param cono
	 * @param faci
	 * @param plgr
	 * @param mere
	 * @param opno
	 * @param mtno
	 * @return true if no error.
	 */
	private boolean checkInputs(Integer cono, String  faci, String prno, Double trq0, Double trq1, Double trq2, Double trq3,
			Double trq4, Double trq5, Double trq6, Double trq7, Double trq8, Double trq9 ) {
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

		if(prno == null) {
			mi.error("Le produit est obligatoire.");
			return false;
		}
		if(!utility.call("CheckUtil", "checkITNOExist", database, cono, prno)) {
			mi.error("Le produit de charge est inexistant");
			return false;
		}

		if(trq0 > 0) {
			mi.error("La quantité de besoin origine 0 doit être négative.");
			return false;
		}
		if(trq1 > 0) {
			mi.error("La quantité de besoin origine 0 doit être négative.");
			return false;
		}
		if(trq2 > 0) {
			mi.error("La quantité de besoin origine 0 doit être négative.");
			return false;
		}
		if(trq3 > 0) {
			mi.error("La quantité de besoin origine 0 doit être négative.");
			return false;
		}
		if(trq4 > 0) {
			mi.error("La quantité de besoin origine 0 doit être négative.");
			return false;
		}
		if(trq5 > 0) {
			mi.error("La quantité de besoin origine 0 doit être négative.");
			return false;
		}
		if(trq6 > 0) {
			mi.error("La quantité de besoin origine 0 doit être négative.");
			return false;
		}
		if(trq7 > 0) {
			mi.error("La quantité de besoin origine 0 doit être négative.");
			return false;
		}
		if(trq8 > 0) {
			mi.error("La quantité de besoin origine 0 doit être négative.");
			return false;
		}
		if(trq9 > 0) {
			mi.error("La quantité de besoin origine 0 doit être négative.");
			return false;
		}

		return true;
	}
}