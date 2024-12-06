/**
 * README
 *
 * Name: AddSlsTicketLin
 * Description: Sauvegarde des lignes de tickets de caisse dans une table spécifique EXT003
 * Date       Changed By                     Description
 * 20240112  François LEPREVOST             Create EXT0035MI_AddSlsTicketLin Transaction
 */
public class AddSlsTicketLin extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database;
  private final ProgramAPI program;
  private final UtilityAPI utility;
  
  public AddSlsTicketLin(MIAPI mi, DatabaseAPI database , ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
	this.mi = mi;
	this.database = database;
	this.program = program;
	this.utility = utility;
  }
  
  int cono = 0;
  String chid = "";
  
  HashMap<String, String> mapDatas = new HashMap<String, String>();
  
  public void main() {
	cono = (Integer) program.getLDAZD().CONO;
	chid = program.getUser();
	
	String divi = (mi.inData.get("DIVI") == null) ? "" : mi.inData.get("DIVI").trim();
	String orno = (mi.inData.get("ORNO") == null) ? "" : mi.inData.get("ORNO").trim();
	String dlix = (mi.inData.get("DLIX") == null) ? "" : mi.inData.get("DLIX").trim();
	String whlo = (mi.inData.get("WHLO") == null) ? "" : mi.inData.get("WHLO").trim();
	String ponr = (mi.inData.get("PONR") == null) ? "" : mi.inData.get("PONR").trim();
	String posx = (mi.inData.get("POSX") == null) ? "" : mi.inData.get("POSX").trim();
	String itrn = (mi.inData.get("ITRN") == null) ? "" : mi.inData.get("ITRN").trim();
	String pbtt = (mi.inData.get("PBTT") == null) ? "0" : mi.inData.get("PBTT").trim();
	String pbht = (mi.inData.get("PBHT") == null) ? "0" : mi.inData.get("PBHT").trim();
	String pntt = (mi.inData.get("PNTT") == null) ? "0" : mi.inData.get("PNTT").trim();
	String pnht = (mi.inData.get("PNHT") == null) ? "0" : mi.inData.get("PNHT").trim();
	String ctar = (mi.inData.get("CTAR") == null) ? "" : mi.inData.get("CTAR").trim();
	String evta = (mi.inData.get("EVTA") == null) ? "" : mi.inData.get("EVTA").trim();
	String vttx = (mi.inData.get("VTTX") == null) ? "" : mi.inData.get("VTTX").trim();
	String vtcd = (mi.inData.get("VTCD") == null) ? "" : mi.inData.get("VTCD").trim();
	String ftco = (mi.inData.get("FTCO") == null) ? "" : mi.inData.get("FTCO").trim();
	
	if (!divi.isEmpty() && !checkDiviExist(divi)) {
	  mi.error("La division est inexistante.");
	  return;
	}
	
	if (dlix.isEmpty()) {
	  mi.error("L'index de livraison est obligatoire.");
	  return;
	}
	
	if (whlo.isEmpty()) {
	  mi.error("Le dépôt est obligatoire.");
	  return;
	} else if (!checkWarehouseExist(whlo)) {
	  mi.error("Le dépôt est inexistant.");
	  return;
	}
	
	int ponrInt = 0;
	int posxInt = 0;
	
	if (ponr.isEmpty()) {
	  mi.error("Le numéro de ligne est obligatoire.");
	  return;
	} else {
	  ponrInt = Integer.parseInt(ponr);
	}
	
	posx = "0";

	if (itrn.isEmpty()) {
	  mi.error("Le numéro de ticket est obligatoire.");
	  return;
	}
	
	if (pbtt.isEmpty()) {
	  pbtt = "0";
	} else if (!utility.call("NumberUtil","isValidNumber", pbtt, ".")) {
	  mi.error("Le prix brut TTC doit être numérique.")
	  return;
	}
	
	if (pbht.isEmpty()) {
	  pbht = "0";
	} else if (!utility.call("NumberUtil","isValidNumber", pbht, ".")) {
	  mi.error("Le prix brut HT doit être numérique.")
	  return;
	}
	
	if (pntt.isEmpty()) {
	  pntt = "0";
	} else if (!utility.call("NumberUtil","isValidNumber", pntt, ".")) {
	  mi.error("Le prix net TTC doit être numérique.")
	  return;
	}
	
	if (pnht.isEmpty()) {
	  pnht = "0";
	} else if (!utility.call("NumberUtil","isValidNumber", pnht, ".")) {
	  mi.error("Le prix net HT doit être numérique.")
	  return;
	}
	
	if (vttx.isEmpty()) {
	  vttx = "0";
	} else if (!utility.call("NumberUtil","isValidNumber", vttx, ".")) {
	  mi.error("Le taux de TVA doit être numérique.")
	  return;
	}
	
	mapDatas.put("divi", divi);
	mapDatas.put("orno", orno);
	mapDatas.put("dlix", dlix);
	mapDatas.put("whlo", whlo);
	mapDatas.put("ponr", ponr);
	mapDatas.put("posx", posx);
	mapDatas.put("itrn", itrn);
	mapDatas.put("pbtt", pbtt);
	mapDatas.put("pbht", pbht);
	mapDatas.put("pntt", pntt);
	mapDatas.put("pnht", pnht);
	mapDatas.put("ctar", ctar);
	mapDatas.put("evta", evta);
	mapDatas.put("vttx", vttx);
	mapDatas.put("vtcd", vtcd);
	mapDatas.put("ftco", ftco);
	
	if (!createEnreg()) {
	  mi.error("L'enregistrement existe déjà !");
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
  * On crée l'enregistrement uniquement sauf si celui-ci existe déjà.
  */
  private boolean createEnreg() {
	long dlixLong = Long.parseLong(mapDatas.get("dlix"));
	int ponrInt = Integer.parseInt(mapDatas.get("ponr"));
	int posxInt = Integer.parseInt(mapDatas.get("posx"));
	long itrnLong = Long.parseLong(mapDatas.get("itrn"));
	double pbttDouble = Double.parseDouble(mapDatas.get("pbtt"));
	double pbhtDouble = Double.parseDouble(mapDatas.get("pbht"));
	double pnttDouble = Double.parseDouble(mapDatas.get("pntt"));
	double pnhtDouble = Double.parseDouble(mapDatas.get("pnht"));
	double vttxDouble = Double.parseDouble(mapDatas.get("vttx"));

	DBAction query = database.table("EXT003").index("00").build();
	DBContainer container = query.getContainer();
	
	container.set("EXCONO", cono);
	container.set("EXDIVI", mapDatas.get("divi"));
	container.set("EXORNO", mapDatas.get("orno"));
	container.set("EXDLIX", dlixLong);
	container.set("EXWHLO", mapDatas.get("whlo"));
	container.set("EXPONR", ponrInt);
	container.set("EXPOSX", posxInt);
	container.set("EXITRN", itrnLong);
	
   if (!query.read(container)) {
	  container.set("EXPBTT", pbttDouble);
	  container.set("EXPBHT", pbhtDouble);
	  container.set("EXPNTT", pnttDouble);
	  container.set("EXPNHT", pnhtDouble);
	  container.set("EXCTAR", mapDatas.get("ctar"));
	  container.set("EXEVTA", mapDatas.get("evta"));
	  container.set("EXVTTX", vttxDouble);
	  container.set("EXVTCD", mapDatas.get("vtcd"));
	  container.set("EXFTCO", mapDatas.get("ftco"));
	  container.set("EXRGDT", utility.call("DateUtil","currentDateY8AsInt"));
	  container.set("EXRGTM", utility.call("DateUtil","currentTimeAsInt"));
	  container.set("EXLMDT", utility.call("DateUtil","currentDateY8AsInt"));
	  container.set("EXCHNO", 1);
	  container.set("EXCHID", chid);
	  
	  query.insert(container);
	  return true;
   } else {
	 return false;
   }
  }
  
}
