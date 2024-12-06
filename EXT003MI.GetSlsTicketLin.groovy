/**
 * README
 *
 * Name: GetSlsTicketLin
 * Description: Récupération d'un ligne de ticket de caisse dans la table spécifique EXT003
 * Date       Changed By                     Description
 * 20240327   François LEPREVOST             Create EXT0035MI_GetSlsTicketLin Transaction
 */
public class GetSlsTicketLin extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database;
  private final ProgramAPI program;
  private final UtilityAPI utility;
  
  public GetSlsTicketLin(MIAPI mi, DatabaseAPI database , ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
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
	String orno = (mi.inData.get("ORNO") == null) ? "" : mi.inData.get("ORNO").trim();
	String dlix = (mi.inData.get("DLIX") == null) ? "" : mi.inData.get("DLIX").trim();
	String whlo = (mi.inData.get("WHLO") == null) ? "" : mi.inData.get("WHLO").trim();
	String ponr = (mi.inData.get("PONR") == null) ? "" : mi.inData.get("PONR").trim();
	String posx = (mi.inData.get("POSX") == null) ? "" : mi.inData.get("POSX").trim();
	String itrn = (mi.inData.get("ITRN") == null) ? "" : mi.inData.get("ITRN").trim();
	
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
	
	if (!searchEnreg()) {
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
  
  private boolean searchEnreg() {
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
	
	Closure<?> releasedProcessor = {
	  DBContainer data ->
	  
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
	
	return query.readAll(container, 8, releasedProcessor);
  }
  
}