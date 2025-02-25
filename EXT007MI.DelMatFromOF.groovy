/**
 * README
 *
 * Name: EXT007MI.DelMatFromOF
 * Description: Enlève quantité matière de l'OF dans la table extma2
 * Date                         Changed By                    Description
 * 20250131                     ddecosterd@hetic3.fr     		création
 * 20250221						ddecosterd@hetic3.fr			fix CHNO not updated. fix field prefix
 */
public class DelMatFromOF extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;
	private final MICallerAPI miCaller;

	public DelMatFromOF(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility, MICallerAPI miCaller) {
		this.mi = mi;
		this.program = program;
		this.database = database;
		this.utility = utility;
		this.miCaller = miCaller;
	}

	public void main() {
		Integer CONO = mi.in.get("CONO");
		String  FACI = (mi.inData.get("FACI") == null) ? "" : mi.inData.get("FACI").trim();
		String PRNO = (mi.inData.get("PRNO") == null) ? "" : mi.inData.get("PRNO").trim();
		String MFNO = (mi.inData.get("MFNO") == null) ? "" : mi.inData.get("MFNO").trim();
		Long SCHN = mi.in.get("SCHN");
		String WHST = (mi.inData.get("WHST") == null) ? "" : mi.inData.get("WHST").trim();

		if(!checkInputs(CONO, FACI, PRNO, MFNO, SCHN)) {
			return;
		}

		if(WHST >= "90")
			return;

		DBAction mwomatRecord = database.table("MWOMAT").index("00").selection("VMPLGR","VMOPNO","VMMTNO","VMREQT").build();
		DBContainer mwomatContainer = mwomatRecord.createContainer();
		mwomatContainer.setInt("VMCONO", CONO);
		mwomatContainer.setString("VMFACI", FACI);
		mwomatContainer.setString("VMPRNO", PRNO);
		mwomatContainer.setString("VMMFNO", MFNO);
		mwomatRecord.readAll(mwomatContainer, 4, 1000, { DBContainer mwomatData ->
			DBAction extma2Record = database.table("EXTMA2").index("00").selection("EXREQT","EXCHNO").build();
			DBContainer extma2Container = extma2Record.createContainer();
			extma2Container.setInt("EXCONO", CONO);
			extma2Container.setString("EXFACI", FACI);
			extma2Container.setString("EXPLGR", mwomatData.getString("VMPLGR"));
			extma2Container.setLong("EXMERE", SCHN);
			extma2Container.setInt("EXOPNO", mwomatData.getInt("VMOPNO"));
			extma2Container.setString("EXMTNO", mwomatData.getString("VMMTNO"));
			extma2Record.readLock(extma2Container, { LockedResult updatedRecord ->
				double newQty = updatedRecord.getDouble("EXREQT") - mwomatData.getDouble("VMREQT");
				if(newQty <= 0 ) {
					updatedRecord.delete();
				}else {
					updatedRecord.setDouble("EXREQT", newQty);
					int CHNO = updatedRecord.getInt("EXCHNO");
					if(CHNO== 999) {CHNO = 0;}
					CHNO++;
					updatedRecord.set("EXLMDT", (Integer) utility.call("DateUtil", "currentDateY8AsInt"));
					updatedRecord.set("EXCHID", program.getUser());
					updatedRecord.setInt("EXCHNO", CHNO);
					updatedRecord.update();
				}
			});
		});

		Map<String,String> parameters =  ["CONO":CONO.toString(),FACI:FACI,MERE:SCHN.toString(),INDX:"20",NDEL:"1"];
		miCaller.call("EXT007MI", "AgregMat", parameters , { Map<String, String> response ->
			if(response.error) {
				mi.error(response.errorMessage);
			}
		});
	}

	/**
	 * Check input values
	 * @param cono
	 * @param faci
	 * @param prno
	 * @param mfno
	 * @param schn
	 * @return true if no error.
	 */
	private boolean checkInputs(Integer cono, String faci, String prno, String mfno, Long schn ) {
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
			mi.error("Le code produit est obligatoire.");
			return false;
		}
		if(!utility.call("CheckUtil", "checkITNOExist", database, cono, prno)) {
			mi.error("Le code produit est inexistant");
			return false;
		}

		if(mfno == null || mfno.isEmpty()) {
			mi.error("Numéro d'OF est obligatoire.");
			return false;
		}

		if(schn == null) {
			mi.error("Le numéro de programme est obligatoire");
			return false;
		}
		if(!utility.call("CheckUtil", "checkSCHNExist", database, cono, schn)){
			mi.error("Le numéro de programme n'existe pas.");
			return false;
		}

		return true;
	}

}
