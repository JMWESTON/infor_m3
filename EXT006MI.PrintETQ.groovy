/**
 * README
 *
 * Name: EXT006MI.PrintETQ
 * Description: Lance l'impression et supprime les données préparées pour l'impression
 * Date                         Changed By                         Description
 * 20240522                     ddecosterd@hetic3.fr     	création
 */public class PrintETQ extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;
	private final MICallerAPI miCaller;

	public PrintETQ(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility, MICallerAPI miCaller) {
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

		ExpressionFactory MPTRNSExpressionFactory = database.getExpressionFactory("MPTRNS");
		MPTRNSExpressionFactory =  MPTRNSExpressionFactory.like("ORPANR",BJNO.toString()+"%");
		DBAction mptrnsRecord = database.table("MPTRNS").index("00").matching(MPTRNSExpressionFactory).build();
		DBContainer mptrnsContainer = mptrnsRecord.createContainer();
		mptrnsContainer.setInt("ORCONO", CONO);
		mptrnsRecord.readAllLock(mptrnsContainer, 1, {  LockedResult lockedRecord ->
			Map<String,String> mms470MIParameters =  [CONO:CONO.toString(),PANR:lockedRecord.getString("ORPANR")];
			miCaller.call("MMS470MI", "PrintPackage", mms470MIParameters , { Map<String, String> response ->
				if(response.containsKey("error")) {
					mi.error(response.errorMessage);
					return;
				}
			});
			lockedRecord.delete();
		});
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
