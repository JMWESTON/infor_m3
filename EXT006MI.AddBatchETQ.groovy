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

			DBAction mpheadRecord = database.table("MPHEAD").selection("IAPUNO").matching(mpheadExpressionFactory).build();
			DBContainer mpheadContainer = mpheadRecord.createContainer();
			mpheadContainer.setInt("IACONO", CONO);

			mpheadRecord.readAll(mpheadContainer, 1, { DBContainer mpheadData ->
				addETQ(CONO, RESP, ITNO, MADI, NBET, mpheadData.getString("IAPUNO"), 0, BJNO);
			});
		}else {
			ExpressionFactory mschmaExpressionFactory = database.getExpressionFactory("MSCHMA");
			mschmaExpressionFactory =  mschmaExpressionFactory.ge("HSSCHN",FSCH.toString()).and(mschmaExpressionFactory.le("HSSCHN", TSCH.toString()));

			DBAction mschmaRecord = database.table("MSCHMA").selection("HSSCHN").matching(mschmaExpressionFactory).build();
			DBContainer  mschmaContainer = mschmaRecord.createContainer();
			mschmaContainer.setInt("HSCONO", CONO);

			mschmaRecord.readAll(mschmaContainer, 1, { DBContainer mschmaData ->
				addETQ(CONO, RESP, ITNO, MADI, NBET, "", mschmaData.getLong("HSSCHN"), BJNO);
			});

		}
	}

	private void addETQ(Integer cono, String resp, String itno, String madi, Integer nbet, String puno, Long schn, String bjno ) {
		Map<String,String> ext006MIParameters =  [CONO:cono.toString(),RESP:resp,ITNO:itno,MADI:madi,NBET:nbet.toString(),PUNO:puno,SCHN:schn.toString(),BJNO:bjno];

		miCaller.call("EXT006MI", "AddETQ", ext006MIParameters , { Map<String, String> response ->
			if(response.containsKey("error")) {
				mi.error(response.errorMessage);
				return;
			}

			this.mi.getOutData().put("CONO", response.CONO);
			this.mi.getOutData().put("NDMD", response.NDMD);
			this.mi.getOutData().put("ITNO", response.ITNO);
			this.mi.getOutData().put("NBET", response.NBET);
			this.mi.getOutData().put("CFI3", response.CFI3);
			this.mi.getOutData().put("ITDS", response.ITDS);
			this.mi.getOutData().put("PUNO", response.PUNO);
			this.mi.getOutData().put("SCHN", response.SCHN);
			this.mi.getOutData().put("RESP", response.RESP);
			this.mi.getOutData().put("IMPR", response.IMPR);
			this.mi.getOutData().put("RGDT", response.RGDT);
			this.mi.getOutData().put("MADI", response.MADI);
			this.mi.write();
		});

	}
}