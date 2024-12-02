/**
 * README
 *
 * Name: EXT002.ChangeSCHN
 * Description: Mise à jour du SCHN pour le OPNO courant et les suivants
 * Date                         Changed By                         Description
 * 20240117                   ddecosterd@hetic3.fr     		Création
 */
public class ChangeSCHN extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;
	private boolean hadError = false;

	public ChangeSCHN(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility) {
		this.mi = mi;
		this.program = program;
		this.database = database;
		this.utility = utility;
	}

	public void main() {

		// Retrieve input fields
		Integer CONO = mi.in.get("CONO");
		String  FACI = (mi.inData.get("FACI") == null) ? "" : mi.inData.get("FACI").trim();
		Integer OPNO = mi.in.get("OPNO");
		Long SCHP = mi.in.get("SCHP");
		Long SCHN = mi.in.get("SCHN");
		String  MFNO = (mi.inData.get("MFNO") == null) ? "" : mi.inData.get("MFNO").trim();

		if(!checkInputs(CONO, FACI, OPNO, SCHP, SCHN))
			return;

		ExpressionFactory mwoopeExpressionFactory = database.getExpressionFactory("MWOOPE");
		mwoopeExpressionFactory = mwoopeExpressionFactory.ge("VOOPNO", OPNO.toString()).and(mwoopeExpressionFactory.eq("MFNO", MFNO));

		DBAction mwoopeRecord = database.table("MWOOPE").matching(mwoopeExpressionFactory).index("90").selection("VOWOST").build();

		DBContainer mwoopeContainer = mwoopeRecord.createContainer();
		mwoopeContainer.set("VOCONO", CONO);
		mwoopeContainer.set("VOFACI", FACI);
		mwoopeContainer.set("VOSCHN", SCHP);

		Closure<?> CheckStatusClosure= { DBContainer MWOOPEdata ->
			if(MWOOPEdata.get("VOWOST").toString() < "20" || MWOOPEdata.get("VOWOST").toString() > "99"){
				mi.error("Incorrect status.");
				hadError = true;
				return;
			}

			ExpressionFactory mwomatExpressionFactory = database.getExpressionFactory("MWOMAT");
			mwomatExpressionFactory = mwomatExpressionFactory.eq("VMSPMT", "1");
			mwomatExpressionFactory = mwomatExpressionFactory.and(mwomatExpressionFactory.eq("MFNO", MFNO));

			DBAction checkWOMAT = database.table("MWOMAT").matching(mwomatExpressionFactory).index("10").build();
			DBContainer mwomatContainer = checkWOMAT.createContainer();
			mwomatContainer.set("VMCONO", MWOOPEdata.get("VOCONO"));
			mwomatContainer.set("VMFACI", MWOOPEdata.get("VOFACI"));
			mwomatContainer.set("VMPRNO", MWOOPEdata.get("VOPRNO"));
			mwomatContainer.set("VMMFNO", MWOOPEdata.get("VOMFNO"));
			mwomatContainer.set("VMOPNO", MWOOPEdata.get("VOOPNO"));

			Closure<?>emptyClosure= { DBContainer MWOMATdata ->
			}

			if(checkWOMAT.readAll(mwomatContainer,5, emptyClosure) > 0) {
				this.mi.error("At Least One component is PL handled");
				hadError = true;
				return;
			}

		}

		mwoopeRecord.readAll(mwoopeContainer, 3, CheckStatusClosure);

		if(hadError)
			return;

		Closure<?> updateClosure = { DBContainer MWOOPEdata ->
			DBAction update = this.database.table("MWOOPE").index("00").build();
			DBContainer mwoopeUpdateContainer = update.createContainer();
			mwoopeUpdateContainer.set("VOCONO", MWOOPEdata.get("VOCONO"));
			mwoopeUpdateContainer.set("VOFACI", MWOOPEdata.get("VOFACI"));
			mwoopeUpdateContainer.set("VOPRNO", MWOOPEdata.get("VOPRNO"));
			mwoopeUpdateContainer.set("VOMFNO", MWOOPEdata.get("VOMFNO"));
			mwoopeUpdateContainer.set("VOOPNO", MWOOPEdata.get("VOOPNO"));

			update.readLock(mwoopeUpdateContainer,{LockedResult updatedRecord ->
				String CHNO = updatedRecord.get("VOCHNO").toString();
				if(CHNO.equals("999")) {CHNO = "0";}
				updatedRecord.set("VOLMDT", (Integer) this.utility.call("DateUtil", "currentDateY8AsInt"));
				updatedRecord.set("VOCHID", this.program.getUser());
				updatedRecord.set("VOCHNO", Integer.parseInt(CHNO)+1);

				updatedRecord.set("VOSCHN", SCHN);
				updatedRecord.update();
			})

			MATHED(mwoopeUpdateContainer, SCHN);
		}

		mwoopeRecord.readAll(mwoopeContainer, 3, updateClosure);

	}

	private boolean checkInputs(Integer cono, String  faci, Integer opno, Long schp, Long schn) {
		if(cono == null) {
			mi.error("La division est obligatoire.");
			return false;
		}
		if(!utility.call("CheckUtil", "checkConoExist", database, cono)) {
			mi.error("La division est inexistante.");
			return false;
		}

		if(faci.isEmpty()) {
			mi.error("L'établissement est obligatoire.");
			return false;
		}
		if(!utility.call("CheckUtil", "checkFacilityExist", database, cono, faci)) {
			mi.error("L'établissement est inexistant.");
			return false;
		}

		if(opno == null) {
			mi.error("Le numéro d'opération est obligatoire.");
			return false;
		}

		if(schp == null) {
			mi.error("Le numéro de programme précédent est obligatoire.");
			return false;
		}
		if(!utility.call("CheckUtil", "checkSCHNExist", database, cono, schp)) {
			mi.error("Le numéro de programme précédent est inexistant");
			return false;
		}

		if(schn == null) {
			mi.error("Le numéro de programme est obligatoire.");
			return false;
		}
		if(!utility.call("CheckUtil", "checkSCHNExist", database, cono, schn)) {
			mi.error("Le numéro de programme est inexistant");
			return false;
		}

		return true;
	}

	/**
	 *    MATHED - Update material with schedulenumber
	 */
	private boolean MATHED(DBContainer mwoopeContainer, Long schn) {
		DBAction mwohedRecord = this.database.table("MWOHED").index("00").build();
		DBContainer mwohedContainer = mwohedRecord.createContainer();
		mwohedContainer.setInt("VHCONO", mwoopeContainer.getInt("VOCONO"));
		mwohedContainer.setString("VHFACI", mwoopeContainer.getString("VOFACI"));
		mwohedContainer.setString("VHMFNO", mwoopeContainer.getString("VOMFNO"));
		mwohedContainer.setString("VHPRNO", mwoopeContainer.getString("VOPRNO"));

		Closure<?> mwohedClosure = { LockedResult updatedRecord ->
			if(updatedRecord.getLong("VHSCHN") != schn) {
				updatedRecord.set("VHSCHN", schn);

				String CHNO = updatedRecord.get("VHCHNO").toString();
				if(CHNO.equals("999")) {CHNO = "0";}

				updatedRecord.set("VHLMDT", (Integer) this.utility.call("DateUtil", "currentDateY8AsInt"));
				updatedRecord.set("VHCHID", this.program.getUser());
				updatedRecord.set("VHCHNO", Integer.parseInt(CHNO)+1);

				updatedRecord.update();
			}
		}

		mwohedRecord.readLock(mwohedContainer, mwohedClosure);

		DBAction mwomatRecord = this.database.table("MWOMAT").index("10").build();
		DBContainer mwomatContainer = mwomatRecord.createContainer();
		mwomatContainer.set("VMCONO", mwoopeContainer.get("VOCONO"));
		mwomatContainer.set("VMFACI", mwoopeContainer.get("VOFACI"));
		mwomatContainer.set("VMMFNO", mwoopeContainer.get("VOMFNO"));
		mwomatContainer.set("VMPRNO", mwoopeContainer.get("VOPRNO"));
		mwomatContainer.set("VMOPNO", mwoopeContainer.get("VOOPNO"));

		Closure<?> mwomatClosure= { DBContainer MWOMATdata ->
			DBAction update = this.database.table("MWOMAT").index("10").build();
			DBContainer MWOMATrecord = update.createContainer();
			MWOMATrecord.set("VMCONO", MWOMATdata.get("VMCONO"));
			MWOMATrecord.set("VMFACI", MWOMATdata.get("VMFACI"));
			MWOMATrecord.set("VMMFNO", MWOMATdata.get("VMMFNO"));
			MWOMATrecord.set("VMPRNO", MWOMATdata.get("VMPRNO"));
			MWOMATrecord.set("VMOPNO", MWOMATdata.get("VMOPNO"));
			MWOMATrecord.set("VMMSEQ", MWOMATdata.get("VMMSEQ"));

			update.readLock(MWOMATrecord,{LockedResult updatedRecord ->
				updatedRecord.set("VMSCHN", schn);

				String CHNO = updatedRecord.get("VMCHNO").toString();
				if(CHNO.equals("999")) {CHNO = "0";}

				updatedRecord.set("VMLMDT", (Integer) this.utility.call("DateUtil", "currentDateY8AsInt"));
				updatedRecord.set("VMCHID", this.program.getUser());
				updatedRecord.set("VMCHNO", Integer.parseInt(CHNO)+1);

				updatedRecord.update();
			})
		}

		return mwomatRecord.readAll(mwomatContainer,5,mwomatClosure)!=0;
	}
}
