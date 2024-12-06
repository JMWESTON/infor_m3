/**
 * README
 *
 * Name: EXT006MI.DelETQ
 * Description: Suppression d'une demande dans la table EXTETQ
 * Date                         Changed By                         Description
 * 20240506                     ddecosterd@hetic3.fr     	création
 */
public class DelETQ extends ExtendM3Transaction {
	private final MIAPI mi;
	private final DatabaseAPI database;
	private final UtilityAPI utility;

	public DelETQ(MIAPI mi, DatabaseAPI database, UtilityAPI utility) {
		this.mi = mi;
		this.database = database;
		this.utility = utility;
	}

	public void main() {
		Integer CONO = mi.in.get("CONO");
		Long NDMD = mi.in.get("NDMD");

		if(CONO == null) {
			mi.error("La division est obligatoire.");
			return;
		}
		if(!this.utility.call("CheckUtil", "checkConoExist", database, CONO)) {
			mi.error("La division est inexistante.");
			return;
		}
		if(NDMD == null) {
			mi.error("Compagny is mandatory.");
		}

		Long BJNO;

		DBAction extetqRecord = database.table("EXTETQ").index("00").build();
		DBContainer extetqContainer = extetqRecord.createContainer();
		extetqContainer.setInt("EXCONO", CONO);
		extetqContainer.setLong("EXNDMD", NDMD);
		if(!extetqRecord.readLock(extetqContainer, { LockedResult record ->
					record.delete();
				})) {
			mi.error("La demande "+NDMD+" n'existe pas.");
			return;
		}
	}

}
