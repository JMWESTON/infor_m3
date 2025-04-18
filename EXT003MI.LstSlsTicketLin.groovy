/**
 * README
 *
 * Name: LstSlsTicketLin
 * Description: Récupération des lignes des tickets de caisse dans la table spécifique EXT003
 * Date       Changed By                     Description
 * 20240327   François LEPREVOST             Create EXT0035MI_LstSlsTicketLin Transaction
 */
public class LstSlsTicketLin extends ExtendM3Transaction {
	private final MIAPI mi;
	private final DatabaseAPI database;
	private final ProgramAPI program;
	private final UtilityAPI utility;

	public LstSlsTicketLin(MIAPI mi, DatabaseAPI database , ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
		this.mi = mi;
		this.database = database;
		this.program = program;
		this.utility = utility;
	}

	int cono = 0;
	HashMap<String, String> mapDatas = new HashMap<String, String>();

	public void main() {
		cono = (Integer) program.getLDAZD().CONO;

		String divi = (mi.inData.get("DIVI") == null) ? "" : mi.inData.get("DIVI").trim();
		String whlo = (mi.inData.get("WHLO") == null) ? "" : mi.inData.get("WHLO").trim();
		String itrn = (mi.inData.get("ITRN") == null) ? "" : mi.inData.get("ITRN").trim();
		String orno = (mi.inData.get("ORNO") == null) ? "" : mi.inData.get("ORNO").trim();
		String dlix = (mi.inData.get("DLIX") == null) ? "" : mi.inData.get("DLIX").trim();
		String ponr = (mi.inData.get("PONR") == null) ? "" : mi.inData.get("PONR").trim();
		String posx = (mi.inData.get("POSX") == null) ? "" : mi.inData.get("POSX").trim();

		if (!divi.isEmpty() && !checkDiviExist(divi)) {
			mi.error("La division est inexistante.");
			return;
		}

		if (whlo.isEmpty()) {
			mi.error("Le dépôt est obligatoire.");
			return;
		} else if (!checkWarehouseExist(whlo)) {
			mi.error("Le dépôt est inexistant.");
			return;
		}

		if (itrn.isEmpty()) {
			itrn = "0";
		}

		if (ponr.isEmpty()) {
			ponr = "0";
		}

		if (posx.isEmpty()) {
			posx = "0";
		}

		if (dlix.isEmpty()) {
			dlix= "0";
		}

		mapDatas.put("divi", divi);
		mapDatas.put("whlo", whlo);
		mapDatas.put("itrn", itrn);
		mapDatas.put("orno", orno);
		mapDatas.put("dlix", dlix);
		mapDatas.put("ponr", ponr);
		mapDatas.put("posx", posx);

		if (!searchEnreg()) {
			mi.error("Aucun enregistrement n'a été trouvé !");
			return;
		}
	}

	/**
	 * On vérifie que la société existe.
	 */
	private boolean checkDiviExist(String divi) {
		DBAction query = database.table("CMNDIV").index("00").build();
		DBContainer container = query.getContainer();
		container.set("CCCONO", cono);
		container.set("CCDIVI", divi);

		return query.read(container);
	}

	/**
	 * On vérifie que le dépôt existe.
	 */
	private boolean checkWarehouseExist(String whlo) {
		DBAction query = database.table("MITWHL").index("00").build();
		DBContainer container = query.getContainer();
		container.set("MWCONO", cono);
		container.set("MWWHLO", whlo);

		return query.read(container);
	}

	/**
	 * Recherche des enregistrements.
	 */
	private boolean searchEnreg() {
		long dlixLong = Long.parseLong(mapDatas.get("dlix"));
		int ponrInt = Integer.parseInt(mapDatas.get("ponr"));
		int posxInt = Integer.parseInt(mapDatas.get("posx"));
		long itrnLong = Long.parseLong(mapDatas.get("itrn"));
		int nbFieldsKey = 3;

		DBAction query = database.table("EXT003").selectAllFields().index("00").build();
		DBContainer container = query.getContainer();

		container.set("EXCONO", cono);
		container.set("EXDIVI", mapDatas.get("divi"));
		container.set("EXWHLO", mapDatas.get("whlo"));

		if (itrnLong != 0L) {
			container.set("EXITRN", itrnLong);
			nbFieldsKey = 4;
		}

		if (!mapDatas.get("orno").isEmpty()) {
			container.set("EXORNO", mapDatas.get("orno"));
			nbFieldsKey = 5;
		}

		if (dlixLong != 0L) {
			container.set("EXDLIX", dlixLong);
			nbFieldsKey = 6;
		}

		if (ponrInt != 0) {
			container.set("EXPONR", ponrInt);
			nbFieldsKey = 7;
		}

		if (ponrInt != 0) {
			container.set("EXPOSX", posxInt);
			nbFieldsKey = 8;
		}

		Closure<?> releasedProcessor = {
			DBContainer data ->

			mi.outData.put("ITRN", String.valueOf(data.get("EXITRN")));
			mi.outData.put("ORNO", String.valueOf(data.get("EXORNO")));
			mi.outData.put("DLIX", String.valueOf(data.get("EXDLIX")));
			mi.outData.put("PONR", String.valueOf(data.get("EXPONR")));
			mi.outData.put("POSX", String.valueOf(data.get("EXPOSX")));
			mi.outData.put("PBTT", String.valueOf(data.get("EXPBTT")));
			mi.outData.put("PBHT", String.valueOf(data.get("EXPBHT")));
			mi.outData.put("PNTT", String.valueOf(data.get("EXPNTT")));
			mi.outData.put("PNHT", String.valueOf(data.get("EXPNHT")));
			mi.outData.put("CTAR", String.valueOf(data.get("EXCTAR")));
			mi.outData.put("EVTA", String.valueOf(data.get("EXEVTA")));
			mi.outData.put("VTTX", String.valueOf(data.get("EXVTTX")));
			mi.outData.put("VTCD", String.valueOf(data.get("EXVTCD")));
			mi.outData.put("FTCO", String.valueOf(data.get("EXFTCO")));
			mi.outData.put("RGDT", String.valueOf(data.get("EXRGDT")));
			mi.outData.put("LMDT", String.valueOf(data.get("EXLMDT")));
			mi.outData.put("RGTM", String.valueOf(data.get("EXRGTM")));
			mi.outData.put("CHID", String.valueOf(data.get("EXCHID")));
			mi.outData.put("CHNO", String.valueOf(data.get("EXCHNO")));
			mi.write();
		}

		int pageSize = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000 ? 10000: mi.getMaxRecords();
		return query.readAll(container, nbFieldsKey, pageSize, releasedProcessor);
	}

}