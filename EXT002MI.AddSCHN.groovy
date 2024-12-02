/**
 * README
 *
 * Name: EXT002.AddSCHN
 * Description: same than option 91 in PMS230 except do not change MWOHED.VHSCHN
 * Date                         Changed By                         Description
 * 20240116                     ddecosterd@hetic3.fr     	création
 */
public class AddSCHN extends ExtendM3Transaction {
	private final MIAPI mi;
	private final ProgramAPI program;
	private final DatabaseAPI database;
	private final UtilityAPI utility;

	public AddSCHN(MIAPI mi, ProgramAPI program, DatabaseAPI database, UtilityAPI utility) {
		this.mi = mi;
		this.program = program;
		this.database = database;
		this.utility = utility;
	}

	public void main() {
		// Retrieve input fields
		Integer CONO = mi.in.get("CONO");
		String  FACI = (mi.inData.get("FACI") == null) ? "" : mi.inData.get("FACI").trim();
		String  MFNO = (mi.inData.get("MFNO") == null) ? "" : mi.inData.get("MFNO").trim();
		String  PRNO = (mi.inData.get("PRNO") == null) ? "" : mi.inData.get("PRNO").trim();
		Integer OPNO = mi.in.get("OPNO");
		Long SCHN = mi.in.get("SCHN");

		if(!checkInputs(CONO, FACI, MFNO, PRNO, OPNO, SCHN))
			return;

		DBAction mwoopeRecord = this.database.table("MWOOPE").index("00").selection("VOWOST").build();

		DBContainer mwoopeContainer = mwoopeRecord.createContainer();
		mwoopeContainer.set("VOCONO", CONO);
		mwoopeContainer.set("VOFACI", FACI);
		mwoopeContainer.set("VOMFNO", MFNO);
		mwoopeContainer.set("VOPRNO", PRNO);
		mwoopeContainer.set("VOOPNO", OPNO);


		if(!mwoopeRecord.read(mwoopeContainer)){
			this.mi.error("No record found.");
			return;
		}

		if(mwoopeContainer.get("VOWOST").toString() < "20" || mwoopeContainer.get("VOWOST").toString() > "99"){
			this.mi.error("Incorrect status.");
			return;
		}

		ExpressionFactory mwomatExpressionFactory = database.getExpressionFactory("MWOMAT");
		mwomatExpressionFactory = mwomatExpressionFactory.eq("VMSPMT", "1");

		DBAction mwomatRecord = this.database.table("MWOMAT").index("10").matching(mwomatExpressionFactory).build();
		DBContainer mwomatContainer = mwomatRecord.createContainer();
		mwomatContainer.set("VMCONO", CONO);
		mwomatContainer.set("VMFACI", FACI);
		mwomatContainer.set("VMMFNO", MFNO);
		mwomatContainer.set("VMPRNO", PRNO);
		mwomatContainer.set("VMOPNO", OPNO);

		Closure<?>emptyClosure= { DBContainer MWOMATdata ->
		}

		if(mwomatRecord.readAll(mwomatContainer,5, emptyClosure) > 0) {
			this.mi.error("At Least One component is PL handled");
			return;
		}

		if(!updateMwoopeSCHN(mwoopeRecord, mwoopeContainer, SCHN)) {
			this.mi.error("No record matching required criterias found.");
			return;
		}

		if(!MATHED(mwomatContainer, SCHN)){
			this.mi.error("No record in MWOMAT matching required criterias found.");
			return;
		}
	}

	private boolean checkInputs(Integer cono, String  faci, String  mfno, String  prno, Integer opno, Long schn) {
		if(cono == null) {
			mi.error("La division est obligatoire.");
			return false;
		}
		if(!this.utility.call("CheckUtil", "checkConoExist", database, cono)) {
			mi.error("La division est inexistante.");
			return false;
		}

		if(faci.isEmpty()) {
			mi.error("L'établissement est obligatoire.");
			return false;
		}
		if(!this.utility.call("CheckUtil", "checkFacilityExist", database, cono, faci)) {
			mi.error("L'établissement est inexistant.");
			return false;
		}

		if(mfno.isEmpty()) {
			mi.error("Le numéro OF est obligatoire.");
			return false;
		}

		if(prno.isEmpty()) {
			mi.error("Le produit est obligatoire.");
			return false;
		}

		if(opno == null) {
			mi.error("Le numéro d'opération est obligatoire.");
			return false;
		}

		if(schn == null) {
			mi.error("Le numéro de programme est obligatoire.");
			return false;
		}
		DBAction query = database.table("MSCHMA").index("00").build();
		DBContainer container = query.getContainer();
		container.set("HSCONO", cono);
		container.set("HSSCHN", schn);
		if(!query.read(container)) {
			mi.error("Le numéro de programme est inexistant");
			return false;
		}

		return true;
	}

	private boolean updateMwoopeSCHN(DBAction mwoopeRecord, DBContainer mwoopeContainer, Long schn) {
		Closure<?> mwoopeClosure= { DBContainer MWOOPEdata ->
			DBAction update = this.database.table("MWOOPE").index("00").build();
			DBContainer MWOOPErecord = update.createContainer();
			MWOOPErecord.set("VOCONO", MWOOPEdata.get("VOCONO"));
			MWOOPErecord.set("VOFACI", MWOOPEdata.get("VOFACI"));
			MWOOPErecord.set("VOMFNO", MWOOPEdata.get("VOMFNO"));
			MWOOPErecord.set("VOPRNO", MWOOPEdata.get("VOPRNO"));
			MWOOPErecord.set("VOOPNO", MWOOPEdata.get("VOOPNO"));

			update.readLock(MWOOPErecord,{LockedResult updatedRecord ->
				updatedRecord.set("VOSCHN", schn);

				String CHNO = updatedRecord.get("VOCHNO").toString();
				if(CHNO.equals("999")) {CHNO = "0";}

				updatedRecord.set("VOLMDT", (Integer) this.utility.call("DateUtil", "currentDateY8AsInt"));
				updatedRecord.set("VOCHID", this.program.getUser());
				updatedRecord.set("VOCHNO", Integer.parseInt(CHNO)+1);

				updatedRecord.update();
			})
		}

		return mwoopeRecord.readAll(mwoopeContainer,5,mwoopeClosure)!=0;
	}

	/**
	 *    MATHED - Update material with schedulenumber
	 */
	private boolean MATHED(DBContainer mwomatContainer, Long schn) {
		DBAction mwomatRecord = this.database.table("MWOMAT").index("10").build();

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

