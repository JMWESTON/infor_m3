/**
 * README
 *
 * Name: EXT006MI.PreparePrint
 * Description: Prépare les données pour l'impression
 * Date                         Changed By                         Description
 * 20240417                     ddecosterd@hetic3.fr     	création
 */
public class PreparePrint extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;
	private final MICallerAPI miCaller;

	public PreparePrint(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility, MICallerAPI miCaller) {
		this.mi = mi;
		this.program = program;
		this.database = database;
		this.utility = utility;
		this.miCaller = miCaller;
	}

	public void main() {
		Integer CONO = mi.in.get("CONO");
		Long BJNO = mi.in.get("BJNO");

		if(!checkInputs(CONO, BJNO))
			return;

		DBAction extetqRecord = database.table("EXTETQ").index("30").selection("EXRESP","EXPUNO","EXSCHN","EXNBET","EXITNO","EXMFNO","EXCFI3").build();
		DBContainer extetqContainer = extetqRecord.createContainer();
		extetqContainer.setInt("EXCONO", CONO);
		extetqContainer.setLong("EXBJNO", BJNO);

		int totalEtiq = 0;
		int totalResp = 0;
		int totalPuno = 0;
		int totalSchn = 0;

		String panr = "";
		String resp = "";
		String puno = "";
		long schn = 0;
		String mfno = "&&ND";
		String itno = "&&ND";
		long ndmd = 0;
		String previousPuno ="";
		Long previousSchn = 0;
		String previousResp ="";
		String lastPuno = "";
		Long lastSchn = 0;
		String lastItno;
		String lastMfno;
		String lastResp ="";
		String lastCFI3;
		int nbet = 0;

		extetqRecord.readAll(extetqContainer, 2, { DBContainer extetqData ->
			if(!resp.equals(extetqData.getString("EXRESP"))) {
				if(!resp.equals("")) {
					totalEtiq = rupture(CONO, totalEtiq, BJNO, lastItno, lastPuno, lastMfno, lastSchn, lastResp, lastCFI3, totalResp, "FIN");
					if(totalEtiq == null)
						return;
					totalResp = 0;
					puno = "";
					schn = 0;
				}
				previousResp = resp;
				resp = extetqData.getString("EXRESP");
			}
			if(!puno.equals(extetqData.getString("EXPUNO"))) {
				if(!puno.equals("")) {
					totalEtiq = rupture(CONO, totalEtiq, BJNO, lastItno, lastPuno, lastMfno, lastSchn, lastResp, lastCFI3, totalPuno, "PUNO");
					if(totalEtiq == null)
						return;
					totalPuno = 0;
					schn = 0;
				}
				previousPuno = puno;
				puno = extetqData.getString("EXPUNO");
			}
			if(schn != extetqData.getLong("EXSCHN")) {
				if(schn != 0) {
					totalEtiq = rupture(CONO, totalEtiq, BJNO, lastItno, lastPuno, lastMfno, lastSchn, lastResp, lastCFI3, totalSchn, "SCHN");
					if(totalEtiq == null)
						return;
					totalSchn = 0;
				}
				previousSchn = schn;
				schn = extetqData.getLong("EXSCHN");
			}

			totalSchn++;
			totalPuno++;
			totalResp++;

			nbet = extetqData.getInt("EXNBET");
			while (nbet > 0) {
				totalEtiq++;
				panr = String.format("%010d",BJNO)+String.format("%010d",totalEtiq);
				if(!addMPTRNS(CONO, extetqData.getString("EXITNO"), puno, extetqData.getString("EXMFNO"), schn, resp, panr, 1, "ITNO", extetqData.getString("EXCFI3"))) {
					return;
				}

				nbet --;
			}

			lastSchn = extetqData.getLong("EXSCHN");
			lastPuno = extetqData.getString("EXPUNO");
			lastItno = extetqData.getString("EXITNO");
			lastMfno = extetqData.getString("EXMFNO");
			lastResp = extetqData.getString("EXRESP");
			lastCFI3 = extetqData.getString("EXCFI3");

		});

		if(lastSchn != 0 && previousSchn != 0 && previousSchn != lastSchn || lastSchn != 0 && (previousResp == "" || previousResp == lastResp)) {
			totalEtiq = rupture(CONO, totalEtiq, BJNO, lastItno, lastPuno, lastMfno, lastSchn, lastResp, lastCFI3, totalSchn, "SCHN");
		}else
			if(lastPuno != "" && previousPuno != "" && previousPuno != lastPuno || lastPuno != "" && (previousResp == "" || previousResp == lastResp)) {
				totalEtiq = rupture(CONO, totalEtiq, BJNO, lastItno, lastPuno, lastMfno, lastSchn, lastResp, lastCFI3, totalPuno, "PUNO");
			}else
				totalEtiq = rupture(CONO, totalEtiq, BJNO, lastItno, lastPuno, lastMfno, lastSchn, lastResp, lastCFI3, totalResp, "RESP");
	}

	private Integer rupture(int cono, int totalEtiq, long bjno, String itno, String puno, String mfno, long schn, String resp, String cfi3, int totalValue, String rupture) {
		totalEtiq++;
		String panr = String.format("%010d",bjno)+String.format("%010d",totalEtiq);
		if(!addMPTRNS(cono, itno, puno, mfno,
				schn, resp, panr, totalValue, rupture, cfi3)) {
			return;
		}
		return totalEtiq;
	}

	private boolean addMPTRNS(int cono, String itno, String puno, String mfno, long schn, String resp, String panr, int gwtm, String rupture, String cfi3 ) {
		boolean isOk = true;
		Map<String,String> mms470MIParameters =  [CONO:cono.toString(),PANR:panr,PACT:"998"];

		miCaller.call("MMS470MI", "AddPackStk", mms470MIParameters , { Map<String, String> response ->
			if(response.containsKey("error")) {
				mi.error(response.errorMessage);
				isOk = false;
				return;
			}
		});

		if(!isOk)
			return isOk;

		String pan3 = resp;
		if(!rupture.equals("ITNO")) {
			if(rupture.equals("PUNO")) {
				pan3 = puno;
			}else if(rupture.equals("SCHN")) {
				pan3 = schn.toString();
			}
			rupture = "FIN";
		}

		mms470MIParameters =  [CONO:cono.toString(),PANR:panr,SORT:schn.toString(),DLRM:itno,DLMO:mfno,ETRN:puno,GWTM:gwtm.toString(),PAN1:rupture,PAN2: cfi3,PAN3:pan3];
		miCaller.call("MMS470MI", "ChangePackStk", mms470MIParameters , { Map<String, String> response ->
			if(response.containsKey("error")) {
				mi.error(response.errorMessage);
				isOk = false;
				return;
			}
		});

		if(!isOk)
			return isOk;

		return isOk;
	}

	/**
	 * Set the change tracking fields
	 * @param insertedRecord the dbcontainer who will be insterted
	 * @param prefix the prefix column
	 */
	private void insertTrackingField(DBContainer insertedRecord, String prefix) {
		insertedRecord.set(prefix+"RGDT", (Integer) this.utility.call("DateUtil", "currentDateY8AsInt"));
		insertedRecord.set(prefix+"LMDT", (Integer) this.utility.call("DateUtil", "currentDateY8AsInt"));
		insertedRecord.set(prefix+"CHID", this.program.getUser());
		insertedRecord.set(prefix+"RGTM", (Integer) this.utility.call("DateUtil", "currentTimeAsInt"));
		insertedRecord.set(prefix+"CHNO", 1);
	}

	private boolean checkInputs(int cono, Long bjno) {
		if(cono == null) {
			mi.error("La division est obligatoire.");
			return false;
		}
		if(!this.utility.call("CheckUtil", "checkConoExist", database, cono)) {
			mi.error("La division est inexistante.");
			return false;
		}

		if(bjno == null) {
			mi.error("Le numéro de mise en impression est obligatoire.");
			return false;
		}
		DBAction extetqRecord = database.table("EXTETQ").index("30").build();
		DBContainer extetqContainer = extetqRecord.createContainer();
		extetqContainer.setInt("EXCONO", cono);
		extetqContainer.setLong("EXBJNO", bjno);
		int nbread = extetqRecord.readAll(extetqContainer, 2, 1, {});
		if(nbread == 0) {
			mi.error("Numéro de mise en impression n'existe pas.");
			return false;
		}

		return true;
	}
}