/**
 * README
 *
 * Name: EXT006MI.AddBatchETQ
 * Description: Appel EXT006MI.AddETQ pour une plage d'OA ou de SCHN
 * Date                         Changed By                         Description
 * 20240430                     ddecosterd@hetic3.fr     	création
 */
public class AddBatchETQ extends ExtendM3Transaction {
	private final MIAPI mi;
	private final DatabaseAPI database;
	private final MICallerAPI miCaller;

	public AddBatchETQ(MIAPI mi, DatabaseAPI database, MICallerAPI miCaller) {
		this.mi = mi;
		this.database = database;
		this.miCaller = miCaller;
	}

	public void main() {
		Integer CONO = mi.in.get("CONO");
		String  RESP = (mi.inData.get("RESP") == null) ? "" : mi.inData.get("RESP").trim();
		String  ITNO = (mi.inData.get("ITNO") == null) ? "" : mi.inData.get("ITNO").trim();
		String  MADI = (mi.inData.get("MADI") == null) ? "" : mi.inData.get("MADI").trim();
		Integer NBET = mi.in.get("NBET");
		String  FPUN = (mi.inData.get("FPUN") == null) ? "" : mi.inData.get("FPUN").trim();
		String  TPUN = (mi.inData.get("TPUN") == null) ? "" : mi.inData.get("TPUN").trim();
		Long FSCH = mi.in.get("FSCH");
		Long TSCH = mi.in.get("TSCH");
		String BJNO = (mi.inData.get("BJNO") == null) ? "" : mi.inData.get("BJNO").trim();

		if(!FPUN.isBlank()) {
			ExpressionFactory mpheadExpressionFactory = database.getExpressionFactory("MPHEAD");
			mpheadExpressionFactory =  mpheadExpressionFactory.ge("IAPUNO",FPUN).and(mpheadExpressionFactory.le("IAPUNO", TPUN));

			DBAction mpheadRecord = database.table("MPHEAD").index("00").selection("IAPUNO").matching(mpheadExpressionFactory).build();
			DBContainer mpheadContainer = mpheadRecord.createContainer();
			mpheadContainer.setInt("IACONO", CONO);

			mpheadRecord.readAll(mpheadContainer, 1, 1000, { DBContainer mpheadData ->
				addETQ(CONO, RESP, ITNO, MADI, NBET, mpheadData.getString("IAPUNO"), 0, BJNO);
			});
		}else {
			ExpressionFactory mschmaExpressionFactory = database.getExpressionFactory("MSCHMA");
			mschmaExpressionFactory =  mschmaExpressionFactory.ge("HSSCHN",FSCH.toString()).and(mschmaExpressionFactory.le("HSSCHN", TSCH.toString()));
			Long limit = TSCH - FSCH;
			if(limit > 1000) {
				mi.error("Intervalle trop grand");
				return;
			}

			DBAction mschmaRecord = database.table("MSCHMA").index("00").selection("HSSCHN").matching(mschmaExpressionFactory).build();
			DBContainer  mschmaContainer = mschmaRecord.createContainer();
			mschmaContainer.setInt("HSCONO", CONO);

			mschmaRecord.readAll(mschmaContainer, 1, limit.intValue(), { DBContainer mschmaData ->
				addETQ(CONO, RESP, ITNO, MADI, NBET, "", mschmaData.getLong("HSSCHN"), BJNO);
			});

		}
	}

	/**
	 * Ajoute les étiquettes dans EXTETQ
	 * @param cono
	 * @param resp
	 * @param itno
	 * @param madi
	 * @param nbet
	 * @param puno
	 * @param schn
	 * @param bjno
	 */
	private void addETQ(Integer cono, String resp, String itno, String madi, Integer nbet, String puno, Long schn, String bjno ) {
		Map<String,String> ext006MIParameters =  [CONO:cono.toString(),RESP:resp,ITNO:itno,MADI:madi,NBET:nbet.toString(),PUNO:puno,SCHN:schn.toString(),BJNO:bjno];

		miCaller.call("EXT006MI", "AddETQ", ext006MIParameters , { Map<String, String> response ->
			if(response.containsKey("error")) {
				mi.error(response.errorMessage);
				return;
			}

			mi.getOutData().put("CONO", response.CONO);
			mi.getOutData().put("NDMD", response.NDMD);
			mi.getOutData().put("ITNO", response.ITNO);
			mi.getOutData().put("NBET", response.NBET);
			mi.getOutData().put("CFI3", response.CFI3);
			mi.getOutData().put("ITDS", response.ITDS);
			mi.getOutData().put("PUNO", response.PUNO);
			mi.getOutData().put("SCHN", response.SCHN);
			mi.getOutData().put("RESP", response.RESP);
			mi.getOutData().put("IMPR", response.IMPR);
			mi.getOutData().put("RGDT", response.RGDT);
			mi.getOutData().put("MADI", response.MADI);
			mi.write();
		});

	}
}