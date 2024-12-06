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

		DBAction extetqRecord = database.table("EXTETQ").index(idx).selectAllFields().build();
		DBContainer extetqContainer = extetqRecord.createContainer();
		extetqContainer.setInt("EXCONO", CONO);
		extetqContainer.setString(fieldName, UTIL);

		extetqRecord.readAll(extetqContainer, 2, { DBContainer extetqData ->
			this.mi.getOutData().put("CONO", extetqData.getInt("EXCONO").toString());
			this.mi.getOutData().put("NDMD", extetqData.getLong("EXNDMD").toString());
			this.mi.getOutData().put("RESP", extetqData.getString("EXRESP"));
			this.mi.getOutData().put("IMPR", extetqData.getString("EXIMPR"));
			this.mi.getOutData().put("ITNO", extetqData.getString("EXITNO"));
			this.mi.getOutData().put("ITDS", extetqData.getString("EXITDS"));
			this.mi.getOutData().put("CFI3", extetqData.getString("EXCFI3"));
			this.mi.getOutData().put("MADI", extetqData.getString("EXMADI"));
			this.mi.getOutData().put("NBET", extetqData.getInt("EXNBET").toString());
			this.mi.getOutData().put("PUNO", extetqData.getString("EXPUNO"));
			this.mi.getOutData().put("SCHN", extetqData.getLong("EXSCHN").toString());
			this.mi.getOutData().put("MFNO", extetqData.getString("EXMFNO"));
			this.mi.getOutData().put("RGDT", extetqData.getInt("EXRGDT").toString());

			DBAction mitmasRecord = database.table("MITMAS").index("00").selection("MMFUDS").build();
			DBContainer mitmasContainer = mitmasRecord.createContainer();
			mitmasContainer.setInt("MMCONO", CONO);
			mitmasContainer.setString("MMITNO", extetqData.getString("EXITNO"));
			mitmasRecord.read(mitmasContainer);
			this.mi.getOutData().put("FUDS", mitmasContainer.getString("MMFUDS"));

			DBAction csytabRecord = database.table("CSYTAB").index("20").selection("CTTX40").build();
			DBContainer csytabContainer = csytabRecord.createContainer();
			csytabContainer.setInt("CTCONO", CONO);
			csytabContainer.setString("CTSTCO", "CFI3");
			csytabContainer.setString("CTSTKY", extetqData.getString("EXCFI3"));
			csytabRecord.readAll(csytabContainer, 3, 1, { DBContainer csytabData ->
				this.mi.getOutData().put("TX40", csytabData.getString("CTTX40"));
			});

			this.mi.write();
		});


	}

	private boolean checkInputs(Integer cono, String util, Integer dmim) {
		if(cono == null) {
			mi.error("La division est obligatoire.");
			return false;
		}
		if(!this.utility.call("CheckUtil", "checkConoExist", database, cono)) {
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