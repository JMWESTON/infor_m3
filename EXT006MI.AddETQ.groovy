/**
 * README
 *
 * Name: EXT006MI.AddETQ
 * Description: Alimentation table EXTETQ
 * Date                         Changed By                         Description
 * 20240415                     ddecosterd@hetic3.fr     	création
 */
public class AddETQ extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;
	private final MICallerAPI miCaller;

	private String cusTxtLibre = "520";
	private String cusGroup = "";
	private String validateur;
	private String imprimante;
	private Integer manufacture;
	private Integer imprimeur;

	public AddETQ(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility, MICallerAPI miCaller) {
		this.mi = mi;
		this.program = program;
		this.database = database;
		this.utility = utility;
		this.miCaller = miCaller;
	}

	public void main() {
		// Retrieve input fields
		Integer CONO = mi.in.get("CONO");
		String  RESP = (mi.inData.get("RESP") == null) ? "" : mi.inData.get("RESP").trim();
		String  ITNO = (mi.inData.get("ITNO") == null) ? "" : mi.inData.get("ITNO").trim();
		String  MADI = (mi.inData.get("MADI") == null) ? "" : mi.inData.get("MADI").trim();
		Integer NBET = mi.in.get("NBET");
		String  PUNO = (mi.inData.get("PUNO") == null) ? "" : mi.inData.get("PUNO").trim();
		Long SCHN = mi.in.get("SCHN");

		if(!checkInputs(CONO, RESP, ITNO, MADI, PUNO, SCHN))
			return;

		init(CONO);

		if(NBET == 0) {
			NBET = 1;
		}
		if(NBET > 111) {
			NBET = 111;
		}
		if(!PUNO.isBlank() || SCHN != null ) {
			NBET = 1;
		}

		if(!ITNO.isBlank()) {
			if(!AJOUT_ETQ(CONO, RESP, ITNO, "", NBET, PUNO, SCHN, MADI))
				return;
		}else if(!PUNO.isBlank()) {
			DBAction mplineRecord = database.table("MPLINE").index("00").selection("IBITNO","IBORQA").build();
			DBContainer mplineContainer = mplineRecord.createContainer();
			mplineContainer.setInt("IBCONO", CONO);
			mplineContainer.setString("IBPUNO", PUNO);

			mplineRecord.readAll(mplineContainer, 2, 1000, { DBContainer mplineData ->
				if(!AJOUT_ETQ(CONO, RESP, mplineData.getString("IBITNO"), "", getNextLong(mplineData.getDouble("IBORQA")), PUNO, SCHN, MADI))
					return;
			});
		}else if(SCHN != null) {
			DBAction mwohedRecord = database.table("MWOHED").index("66").selection("VHPRNO", "VHMFNO", "VHORQT").build();
			DBContainer mwohedContainer = mwohedRecord.createContainer();
			mwohedContainer.setInt("VHCONO", CONO);
			mwohedContainer.setLong("VHSCHN", SCHN);

			mwohedRecord.readAll(mwohedContainer, 2, 1000, { DBContainer mwohedData ->
				if(!AJOUT_ETQ(CONO, RESP, mwohedData.getString("VHPRNO"), mwohedData.getString("VHMFNO"), getNextLong(mwohedData.getDouble("VHORQT")), PUNO, SCHN, MADI))
					return;
			});
		}
	}

	private long getNextLong(double value) {
		double truncated = value.trunc();
		return value>truncated ? truncated + 1 : truncated;
	}

	/**
	 * Ajout étiquette à la table EXTETQ
	 * @param cono
	 * @param resp
	 * @param itno
	 * @param mfno
	 * @param nbet
	 * @param puno
	 * @param schn
	 * @param madi
	 * @return true if no error
	 */
	private boolean AJOUT_ETQ(int cono, String resp, String itno, String mfno, long nbet, String puno, Long schn, String madi) {
		boolean result = false;
		DBAction mitmasRecord = database.table("MITMAS").index("00").selection("MMHIE3","MMCFI3", "MMITDS").build();
		DBContainer mitmasContainer = mitmasRecord.createContainer();
		mitmasContainer.setInt("MMCONO", cono);
		mitmasContainer.setString("MMITNO", itno);
		mitmasRecord.read(mitmasContainer);

		String hier3 = mitmasContainer.getString("MMHIE3");
		String cfi3 = mitmasContainer.getString("MMCFI3");
		String itds = mitmasContainer.getString("MMITDS");
		if(schn == null)
			schn = 0;

		result = writeRecord(cono, itno, nbet.toInteger(), mfno, resp, cfi3, itds, puno, schn, madi);

		//Cas particulier des bottes
		if(schn > 0 && result && hier3.equals("01002001")) {
			result = writeRecord(cono, itno, nbet.toInteger(), mfno, resp, cfi3, itds, puno, schn, madi);
			if(result)
				writeRecord(cono, itno, nbet.toInteger(), mfno, resp, "05", itds, puno, schn, madi);
		}

		return result;
	}

	/**
	 * write record in EXTETQ
	 * @param cono
	 * @param itno
	 * @param nbet
	 * @param mfno
	 * @param resp
	 * @param cfi3
	 * @param itds
	 * @param puno
	 * @param schn
	 * @param madi
	 * @return true if no error
	 */
	private boolean writeRecord(int cono, String itno, int nbet, String mfno, String resp, String cfi3, String itds, String puno, Long schn, String madi) {
		Long ndmd = getNdmd();
		if (ndmd == null)
			return false;

		DBAction extetqRecord = database.table("EXTETQ").index("00").build();
		DBContainer extetqContainer = extetqRecord.createContainer();
		extetqContainer.setInt("EXCONO", cono);
		extetqContainer.setString("EXITNO", itno);
		extetqContainer.setLong("EXNDMD", ndmd);
		extetqContainer.setInt("EXNBET", nbet);
		extetqContainer.setString("EXMFNO", mfno);
		extetqContainer.setString("EXRESP", resp);
		extetqContainer.setString("EXIMPR", validateur);
		extetqContainer.setString("EXCFI3", cfi3);
		extetqContainer.setString("EXITDS", itds);
		extetqContainer.setString("EXPUNO", puno);
		extetqContainer.setLong("EXSCHN", schn);
		extetqContainer.setString("EXMADI", madi);
		insertTrackingField(extetqContainer, "EX");
		extetqRecord.insert(extetqContainer);

		mi.getOutData().put("CONO", cono.toString());
		mi.getOutData().put("NDMD", ndmd.toString());
		mi.getOutData().put("ITNO", itno);
		mi.getOutData().put("NBET", nbet.toString());
		mi.getOutData().put("CFI3", cfi3);
		mi.getOutData().put("ITDS", itds);
		mi.getOutData().put("RESP", resp);
		mi.getOutData().put("PUNO", puno);
		mi.getOutData().put("SCHN", schn.toString());
		mi.getOutData().put("MADI", madi);
		mi.getOutData().put("IMPR", validateur);
		mi.getOutData().put("RGDT", utility.call("DateUtil", "currentDateY8AsInt").toString());
		mi.write();

		return true;
	}

	/**
	 * Get new demand number
	 * @return a new demand number
	 */
	private Long getNdmd() {
		Long result = null;
		Map<String,String> crs165MIParameters =  [NBTY:"Z2",NBID:"1"];

		miCaller.call("CRS165MI", "RtvNextNumber", crs165MIParameters , { Map<String, String> response ->
			if(response.containsKey("error")) {
				mi.error(response.errorMessage);
				return;
			}
			result = Long.parseLong(response.NBNR.trim());
		});
		return result;
	}

	/**
	 * Get config values
	 * @param cono
	 */
	private void init(int cono) {
		DBAction CUGEX1Record = database.table("CUGEX1").index("00").selection("F1A030","F1A130").build();
		DBContainer CUGEX1Container = CUGEX1Record.createContainer();
		CUGEX1Container.setInt("F1CONO", cono);
		CUGEX1Container.setString("F1FILE", "H5SDK");
		CUGEX1Container.setString("F1PK01", "PRD009");
		if(CUGEX1Record.read(CUGEX1Container)) {
			cusTxtLibre = CUGEX1Container.getString("F1A030");
			cusGroup = CUGEX1Container.getString("F1A130");
		}else {
			CUGEX1Container.setString("F1A030",cusTxtLibre);
			CUGEX1Container.setString("F1A130",cusGroup);
			insertTrackingField(CUGEX1Container, "F1");
			CUGEX1Record.insert(CUGEX1Container);
		}

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
	 * @param resp
	 * @param itno
	 * @param madi
	 * @param puno
	 * @param schn
	 * @return true if no error.
	 */
	private boolean checkInputs(Integer cono, String resp, String  itno, String madi, String puno, Long schn) {
		if(cono == null) {
			mi.error("La division est obligatoire.");
			return false;
		}
		if(!utility.call("CheckUtil", "checkConoExist", database, cono)) {
			mi.error("La division est inexistante.");
			return false;
		}

		DBAction CUGEX1Record = database.table("CUGEX1").index("00").selection("F1A030","F1N096","F1N196","F1CHB1","F1CHB2").build();
		DBContainer CUGEX1Container = CUGEX1Record.createContainer();
		CUGEX1Container.setInt("F1CONO", cono);
		CUGEX1Container.setString("F1FILE", "PRD009");
		CUGEX1Container.setString("F1PK01", resp);
		if(CUGEX1Record.read(CUGEX1Container)) {
			validateur = CUGEX1Container.getString("F1A030");
			imprimante = CUGEX1Container.getString("F1A130");
			imprimeur = CUGEX1Container.getInt("F1CHB1");
			manufacture = CUGEX1Container.getInt("F1CHB2");
		}else {
			mi.error("Responsable inexistant");
			return false;
		}

		if(itno.isBlank() && (schn == null || schn == 0) && puno.isBlank()) {
			mi.error("Il faut renseigner soit l'article, soit l'OA soit la note.");
			return false;
		}
		if(!puno.isBlank()&& !madi.isBlank()) {
			mi.error("Si numéro d'OA est renseigné il ne faut pas renseigner Made In.");
			return false;
		}

		if(!itno.isBlank() && !utility.call("CheckUtil", "checkITNOExist", database, cono, itno)) {
			mi.error("L'article est inexistant.");
			return false;
		}

		if(!puno.isBlank() && !utility.call("CheckUtil", "checkPUNOExist", database, cono, puno)) {
			mi.error("Numéro d'OA inexistant.");
			return false;
		}

		if(!puno.isBlank()&& !itno.isBlank()) {
			mi.error("Si numéro d'OA est renseigné il ne faut pas renseigner l'article.");
			return false;
		}
		if(!puno.isBlank()&& (schn != null && schn != 0)) {
			mi.error("Si numéro d'OA est renseigné il ne faut pas renseigner le numéro de note.");
			return false;
		}

		if(schn != null && schn != 0 && !utility.call("CheckUtil", "checkSCHNExist", database, cono, schn)) {
			mi.error("Note inexistante");
			return false;
		}

		if(schn != null && schn > 0 && !madi.isBlank()) {
			mi.error("Si note renseignée il ne faut pas renseigner Made In.");
			return false;
		}

		return true;
	}
}