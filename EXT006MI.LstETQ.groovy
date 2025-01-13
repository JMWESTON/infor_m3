/**
 * README
 *
 * Name: EXT006MI.LstETQ
 * Description: List des demandes pour un imprimeur
 * Date                         Changed By                         Description
 * 20240417                     ddecosterd@hetic3.fr     	création
 */
public class LstETQ extends ExtendM3Transaction {
	private final MIAPI mi;
	private final DatabaseAPI database;
	private final UtilityAPI utility;

	public LstETQ(MIAPI mi, DatabaseAPI database, UtilityAPI utility) {
		this.mi = mi;
		this.database = database;
		this.utility = utility;
	}

	public void main() {
		Integer CONO = mi.in.get("CONO");
		String  UTIL = (mi.inData.get("UTIL") == null) ? "" : mi.inData.get("UTIL").trim();
		Integer DMIM = mi.in.get("DMIM");

		if(!checkInputs(CONO, UTIL, DMIM))
			return;

		String idx;
		String fieldName;
		if(DMIM == 1) {
			idx = 10;
			fieldName = "EXIMPR";
		}else {
			idx = 20;
			fieldName = "EXRESP";
		}

		DBAction extetqRecord = database.table("EXTETQ").index(idx).selection("EXCONO","EXNDMD","EXRESP","EXIMPR","EXITNO","EXITDS","EXCFI3","EXMADI","EXNBET","EXPUNO","EXPUNO",
				"EXSCHN","EXMFNO","EXRGDT").build();
		DBContainer extetqContainer = extetqRecord.createContainer();
		extetqContainer.setInt("EXCONO", CONO);
		extetqContainer.setString(fieldName, UTIL);

		extetqRecord.readAll(extetqContainer, 2, 2000, { DBContainer extetqData ->
			mi.getOutData().put("CONO", extetqData.getInt("EXCONO").toString());
			mi.getOutData().put("NDMD", extetqData.getLong("EXNDMD").toString());
			mi.getOutData().put("RESP", extetqData.getString("EXRESP"));
			mi.getOutData().put("IMPR", extetqData.getString("EXIMPR"));
			mi.getOutData().put("ITNO", extetqData.getString("EXITNO"));
			mi.getOutData().put("ITDS", extetqData.getString("EXITDS"));
			mi.getOutData().put("CFI3", extetqData.getString("EXCFI3"));
			mi.getOutData().put("MADI", extetqData.getString("EXMADI"));
			mi.getOutData().put("NBET", extetqData.getInt("EXNBET").toString());
			mi.getOutData().put("PUNO", extetqData.getString("EXPUNO"));
			mi.getOutData().put("SCHN", extetqData.getLong("EXSCHN").toString());
			mi.getOutData().put("MFNO", extetqData.getString("EXMFNO"));
			mi.getOutData().put("RGDT", extetqData.getInt("EXRGDT").toString());

			DBAction mitmasRecord = database.table("MITMAS").index("00").selection("MMFUDS").build();
			DBContainer mitmasContainer = mitmasRecord.createContainer();
			mitmasContainer.setInt("MMCONO", CONO);
			mitmasContainer.setString("MMITNO", extetqData.getString("EXITNO"));
			mitmasRecord.read(mitmasContainer);
			mi.getOutData().put("FUDS", mitmasContainer.getString("MMFUDS"));

			DBAction csytabRecord = database.table("CSYTAB").index("20").selection("CTTX40").build();
			DBContainer csytabContainer = csytabRecord.createContainer();
			csytabContainer.setInt("CTCONO", CONO);
			csytabContainer.setString("CTSTCO", "CFI3");
			csytabContainer.setString("CTSTKY", extetqData.getString("EXCFI3"));
			csytabRecord.readAll(csytabContainer, 3, 1, { DBContainer csytabData ->
				mi.getOutData().put("TX40", csytabData.getString("CTTX40"));
			});

			mi.write();
		});


	}

	/**
	 * Check input values
	 * @param cono
	 * @param util
	 * @param dmim
	 * @return true if no error.
	 */
	private boolean checkInputs(Integer cono, String util, Integer dmim) {
		if(cono == null) {
			mi.error("La division est obligatoire.");
			return false;
		}
		if(!utility.call("CheckUtil", "checkConoExist", database, cono)) {
			mi.error("La division est inexistante.");
			return false;
		}

		if(dmim == null) {
			mi.error("Le type d'utilisateur est obligatoire.");
			return false;
		}

		if(dmim !=0 &&  dmim !=1 ) {
			mi.error("Le type utilisateur ne peut être que 0 ou 1.");
			return false;
		}

		if(util == null) {
			mi.error("Le code utilisateur est obligatoire.");
			return false;
		}

		return true;
	}
}