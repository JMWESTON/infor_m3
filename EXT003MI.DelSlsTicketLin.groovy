/**
 * README
 *
 * Name: DelSlsTicketLin
 * Description: Suppression d'un ligne de ticket de caisse dans la table spécifique EXT003
 * Date       Changed By                     Description
 * 20240328   François LEPREVOST             Create EXT0035MI_DelSlsTicketLin Transaction
 */

public class DelSlsTicketLin extends ExtendM3Transaction {
	private final MIAPI mi;
	private final DatabaseAPI database;
	private final ProgramAPI program;
	private final UtilityAPI utility;

	public DelSlsTicketLin(MIAPI mi, DatabaseAPI database , ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
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

		if (itrn.isEmpty()) {
			mi.error("Le numéro de ticket est obligatoire.");
			return;
		}

		if (whlo.isEmpty()) {
			mi.error("Le dépôt est obligatoire.");
			return;
		} else if (!checkWarehouseExist(whlo)) {
			mi.error("Le dépôt est inexistant.");
			return;
		}

		if (ponr.isEmpty()) {
			mi.error("Le numéro de ligne est obligatoire.");
			return;
		}

		if (posx.isEmpty()) {
			posx = "0";
		}

		mapDatas.put("divi", divi);
		mapDatas.put("orno", orno);
		mapDatas.put("dlix", dlix);
		mapDatas.put("whlo", whlo);
		mapDatas.put("ponr", ponr);
		mapDatas.put("posx", posx);
		mapDatas.put("itrn", itrn);

		if (!deleteEnreg()) {
			mi.error("Enregistrement inexistant !");
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

	private boolean deleteEnreg() {
		long dlixLong = Long.parseLong(mapDatas.get("dlix"));
		int ponrInt = Integer.parseInt(mapDatas.get("ponr"));
		int posxInt = Integer.parseInt(mapDatas.get("posx"));
		long itrnLong = Long.parseLong(mapDatas.get("itrn"));

		DBAction query = database.table("EXT003").selectAllFields().index("00").build();
		DBContainer container = query.getContainer();

		container.set("EXCONO", cono);
		container.set("EXDIVI", mapDatas.get("divi"));
		container.set("EXORNO", mapDatas.get("orno"));
		container.set("EXDLIX", dlixLong);
		container.set("EXWHLO", mapDatas.get("whlo"));
		container.set("EXPONR", ponrInt);
		container.set("EXPOSX", posxInt);
		container.set("EXITRN", itrnLong);

		Closure<?> deleterCallback = { LockedResult lockedResult ->
			lockedResult.delete();
		}

		return  query.readLock(container, deleterCallback);
	}
}