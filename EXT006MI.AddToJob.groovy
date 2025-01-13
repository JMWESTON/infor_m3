/**
 * README
 *
 * Name: EXT006MI.AddToJob
 * Description: Ajout d'une étiquette dans une demande d'impression
 * Date                         Changed By                         Description
 * 20240417                     ddecosterd@hetic3.fr     	création
 */
public class AddToJob extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;

	public AddToJob(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility) {
		this.mi = mi;
		this.program = program;
		this.database = database;
		this.utility = utility;
	}

	public void main() {
		Integer CONO = mi.in.get("CONO");
		Long NDMD = mi.in.get("NDMD");
		Long BJNO = mi.in.get("BJNO");

		if(!checkInputs(CONO, NDMD, BJNO))
			return;

		DBAction extetqRecord = database.table("EXTETQ").index("30").build();
		DBContainer extetqContainer = extetqRecord.createContainer();
		extetqContainer.setInt("EXCONO", CONO);
		extetqContainer.setLong("EXBJNO", BJNO);
		String cfi3;

		int nbread = extetqRecord.readAll(extetqContainer, 2, 1, { DBContainer extetqData ->
			cfi3 = extetqData.getString("EXCFI3");
		});

		extetqRecord = database.table("EXTETQ").index("00").build();
		extetqContainer = extetqRecord.createContainer();
		extetqContainer.setInt("EXCONO", CONO);
		extetqContainer.setLong("EXNDMD", NDMD);

		extetqRecord.readLock(extetqContainer, {LockedResult updatedRecord ->
			if(nbread > 0 && cfi3 != updatedRecord.getString("EXCFI3") ) {
				mi.error("Format étiquette incohérent.");
			}else {
				updatedRecord.setLong("EXBJNO", BJNO);
				updateTrackingField(updatedRecord, "EX");
				updatedRecord.update();
			}
		});

	}


	/**
	 * Check input values
	 * @param cono
	 * @param ndmd
	 * @param bjno
	 * @return true if no error.
	 */
	private boolean checkInputs(int cono, Long ndmd, Long bjno) {
		if(cono == null) {
			mi.error("La division est obligatoire.");
			return false;
		}
		if(!utility.call("CheckUtil", "checkConoExist", database, cono)) {
			mi.error("La division est inexistante.");
			return false;
		}

		if(ndmd == null) {
			mi.error("Le numéro de demande est obligatoire");
			return false;
		}

		DBAction extetqRecord = database.table("EXTETQ").index("00").build();
		DBContainer extetqContainer = extetqRecord.createContainer();
		extetqContainer.setInt("EXCONO", cono);
		extetqContainer.setLong("EXNDMD", ndmd);
		if(!extetqRecord.read(extetqContainer)) {
			mi.error("Numéro de demande inexistant.");
			return false;
		}

		if(bjno == null) {
			mi.error("Le numéro de mise en impression est obligatoire.");
			return false;
		}

		return true;
	}

	/**
	 *  Add default value for updated record.
	 * @param updatedRecord
	 * @param prefix The column prefix of the table.
	 */
	private void updateTrackingField(LockedResult updatedRecord, String prefix) {
		int CHNO = updatedRecord.getInt(prefix+"CHNO");
		if(CHNO== 999) {CHNO = 0;}
		updatedRecord.set(prefix+"LMDT", (Integer) utility.call("DateUtil", "currentDateY8AsInt"));
		updatedRecord.set(prefix+"CHID", program.getUser());
		updatedRecord.setInt(prefix+"CHNO", CHNO+1);
	}
}