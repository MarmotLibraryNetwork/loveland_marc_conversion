package org.marmot;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.marc4j.MarcPermissiveStreamReader;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamWriter;
import org.marc4j.marc.*;
import org.marc4j.marc.impl.DataFieldImpl;
import org.marc4j.marc.impl.MarcFactoryImpl;
import org.marc4j.marc.impl.SubfieldImpl;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Description goes here
 * Longmont Marc Conversion
 * User: Mark Noble
 * Date: 10/14/13
 * Time: 3:51 PM
 */
public class ConvertMarcRecordsMain {
	private static Logger logger	= Logger.getLogger(ConvertMarcRecordsMain.class);

	public static void main(String[] args){
		// Initialize the logger
		File log4jFile = new File("./log4j.properties");
		if (log4jFile.exists()) {
			PropertyConfigurator.configure(log4jFile.getAbsolutePath());
		} else {
			System.out.println("Could not find log4j configuration " + log4jFile.getAbsolutePath());
			System.exit(1);
		}
		logger.warn("Starting conversion " + new Date().toString());
		//Connect to the database
		Connection dbConnection = null;
		try{
			dbConnection = DriverManager.getConnection("jdbc:mysql://localhost/flatirons_new_member_loads?user=root&password=M4rm0T&useUnicode=yes&characterEncoding=UTF-8");
		}catch (Exception e){
			logger.error("Error connecting to database ", e);
			System.exit(1);
		}

		//Load MARC Existing MARC Records from Sierra?
		FRBRProcessor frbrIzer = new FRBRProcessor(dbConnection, logger);
		boolean groupTitlesFromCatalog = true;
		if (groupTitlesFromCatalog){
			//Clear the database first
			PreparedStatement addBarcodeToBibStmt = null;
			try{
				dbConnection.prepareStatement("TRUNCATE barcode_to_bib").executeUpdate();
				dbConnection.prepareStatement("TRUNCATE grouped_record").executeUpdate();
				dbConnection.prepareStatement("TRUNCATE grouped_record_related_bibs").executeUpdate();
				dbConnection.prepareStatement("TRUNCATE grouped_record_identifiers").executeUpdate();

				addBarcodeToBibStmt = dbConnection.prepareStatement("INSERT IGNORE INTO barcode_to_bib (barcode, bib, itemId) VALUES (?, ?, ?)");
			}catch (Exception e){
				logger.error("Error clearing database ", e);
				System.exit(1);
			}

			File[] catalogBibFiles = new File("C:\\web\\loveland_marc_conversion\\catalog_bibs\\").listFiles();

			int numCatalogBibs = 0;
			for (File curBibFile : catalogBibFiles){
				if (curBibFile.getName().endsWith(".mrc")){
					logger.info("Processing " + curBibFile);
					try{
						MarcReader catalogReader = new MarcPermissiveStreamReader(new FileInputStream(curBibFile), true, true, "UTF8");
						while (catalogReader.hasNext()){
							Record curBib = catalogReader.next();
							String bibNumber = "";
							List<DataField> bibNumberFields = curBib.getVariableFields("907");
							for (DataField bibNumberField : bibNumberFields){
								if (bibNumberField.getSubfield('a').getData().matches("\\.b.*")){
									bibNumber = bibNumberField.getSubfield('a').getData();
									break;
								}
							}
							if (bibNumber.length() == 0){
								logger.warn("Could not find bib number for record");
							} else {
								//Save a list of barcodes for each bib for loveland data so we can process item level holds properly
								List<DataField> itemFields = curBib.getVariableFields("945");
								for (DataField itemField : itemFields) {
									if (itemField.getSubfield('i') != null) {
										String barcode = itemField.getSubfield('i').getData().trim();
										String itemId = itemField.getSubfield('y').getData().trim();
										String location = itemField.getSubfield('l').getData().trim();
										if (barcode.length() > 0 && location.startsWith("la")) {
											try {
												addBarcodeToBibStmt.setString(1, barcode);
												addBarcodeToBibStmt.setString(2, bibNumber);
												addBarcodeToBibStmt.setString(3, itemId);
												addBarcodeToBibStmt.executeUpdate();
											} catch (Exception e) {
												logger.error("Error adding barcode to bib to database", e);
											}
										}
									}
								}

								if (itemFields.size() > 0){
									frbrIzer.addRecordToWorks(curBib);
								}
							}
							numCatalogBibs++;
						}
					}catch(Exception e){
						logger.error("Error loading catalog bibs: ", e);
						e.printStackTrace();

					}
				}
			}
			//System.out.println("Read " + numCatalogBibs + " from the catalog.  There are " + frbrIzer.getNumWorks() + " works in the catalog.");
			logger.warn("Finished grouping " + numCatalogBibs + " records from catalog.");
		}

		loadCollectionToLocationMap();
		loadStatusMap();
		loadItemTypeMap();

		//Load MARC Records from loveland Extract
		File lovelandMarcFile = new File("C:\\web\\loveland_marc_conversion\\bibexport92316.mrc");
		File matchedRecordDestination = new File("C:\\web\\loveland_marc_conversion\\results\\exact_match_records.mrc");
		File fuzzyRecordDestination = new File("C:\\web\\loveland_marc_conversion\\results\\fuzzy_match_records.mrc");
		File uniqueRecordDestination = new File("C:\\web\\loveland_marc_conversion\\results\\unique_records.mrc");
		File uniqueLargeRecordDestination = new File("C:\\web\\loveland_marc_conversion\\results\\unique_records_large.mrc");
		File specialStatusItemsDestination = new File("C:\\web\\loveland_marc_conversion\\results\\special_status_items.mrc");
		File specialStatusBarcodesDestination = new File("C:\\web\\loveland_marc_conversion\\results\\special_status_barcodes.txt");
		File uniqueCopiesDestination = new File("C:\\web\\loveland_marc_conversion\\results\\unique_copies.txt");
		File recordsWithLcCallNumbersDestination = new File("C:\\web\\loveland_marc_conversion\\results\\records_with_lc_callnumbers.txt");

		File iTypesNotConverted = new File("C:\\web\\loveland_marc_conversion\\results\\unhandled_itypes.csv");
		File collectionsNotConverted = new File("C:\\web\\loveland_marc_conversion\\results\\unhandled_collections.csv");
		File statusesNotConverted = new File("C:\\web\\loveland_marc_conversion\\results\\unhandled_statuses.csv");
		File invalidRecordsFile = new File("C:\\web\\loveland_marc_conversion\\results\\invalid_records.csv");
		File lostAndPaidItemsFile = new File("C:\\web\\loveland_marc_conversion\\results\\lost_and_paid_items.csv");
		File claimsReturnedItemsFile = new File("C:\\web\\loveland_marc_conversion\\results\\claims_returned_items.csv");
		File billedItemsFile = new File("C:\\web\\loveland_marc_conversion\\results\\billed_items.csv");

		String curDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());

		TreeSet<String> uniqueCopyData = new TreeSet<>();
		TreeSet<String> recordsWithLcCallNumbers = new TreeSet<>();
		ArrayList<String> invalidRecords = new ArrayList<>();
		HashMap<String, Long> unknownStatuses = new HashMap<>();
		HashMap<String, Long> unknownITypes= new HashMap<>();
		HashMap<String, Long> unknownCollections = new HashMap<>();
		HashSet<String> unknownCallNumberTypes = new HashSet<>();

		TreeSet<String> lostAndPaidItems = new TreeSet<>();
		TreeSet<String> claimsReturnedItems = new TreeSet<>();
		TreeSet<String> billedItems = new TreeSet<>();

		try{
			InputStream input = new FileInputStream(lovelandMarcFile);
			MarcReader reader = new MarcPermissiveStreamReader(input, false, true);

			MarcStreamWriter matchedRecordWriter = new MarcStreamWriter(new FileOutputStream(matchedRecordDestination), "UTF8");
			MarcStreamWriter fuzzyMatchedRecordWriter = new MarcStreamWriter(new FileOutputStream(fuzzyRecordDestination), "UTF8");
			MarcStreamWriter uniqueRecordWriter = new MarcStreamWriter(new FileOutputStream(uniqueRecordDestination), "UTF8");
			MarcStreamWriter uniqueLargeRecordWriter = new MarcStreamWriter(new FileOutputStream(uniqueLargeRecordDestination), "UTF8");
			MarcStreamWriter specialStatusItemsWriter = new MarcStreamWriter(new FileOutputStream(specialStatusItemsDestination), "UTF8");
			FileWriter specialStatusBarcodesWriter = new FileWriter(specialStatusBarcodesDestination);

			HashMap<String, TreeSet<String>> barcodesWithSpecialStatuses = new HashMap<>();

			int numRecordsRead = 0;
			int numUniqueBibs = 0;
			int numMatchedBibs = 0;
			int numFuzzyMatchBibs = 0;
			int numProjectGutenberg = 0;
			int numOverdrive = 0;
			String lastRecordId = "None";

			//Process each record
			while (reader.hasNext()){
				Record record;
				boolean forceUnique = false;
				boolean hasSpecialStatusItems = false;
				numRecordsRead++;
				try {
					record = reader.next();
				}catch (Exception e){
					logger.warn("Could not read record, last record read was number " + numRecordsRead + " id " + lastRecordId);
					invalidRecords.add("Could not read record " + numRecordsRead + " previous record id was " + lastRecordId + " " + e.toString());
					continue;
				}

				//Check to see if this is a Gutenberg record and if so skip it.
				DataField field710 = (DataField)record.getVariableField("710");
				if (field710 != null && field710.getSubfield('a') != null && field710.getSubfield('a').getData().matches("Project Gutenberg.*")){
					//System.out.println("removing gutenberg title");
					numProjectGutenberg++;
					continue;
				}

				DataField field856 = (DataField)record.getVariableField("856");
				//suppress overdrive, one click, hoopla
				if (field856 != null && field856.getSubfield('u') != null
						&& field856.getSubfield('u').getData().matches(".*\\.lib\\.overdrive.*")
						&& field856.getSubfield('u').getData().matches(".*hoopla.*")
						&& field856.getSubfield('u').getData().matches(".*oneclickdigital.*")
				){
					//System.out.println("removing gutenberg title");
					numOverdrive++;
					continue;
				}

				//Update 040 to match flatirons standards
				DataField field040 = (DataField)record.getVariableField("040");
				if (field040 != null){
					field040.addSubfield(new SubfieldImpl('d', "CoBoFLC"));
				}else{
					field040 = new DataFieldImpl("040", ' ', ' ');
					field040.addSubfield(new SubfieldImpl('d', "CoBoFLC"));
					record.addVariableField(field040);
				}

				//Remove fields that should not be imported
				//Remove 049 for flatirons
				String[] fieldNumbersToRemove = new String[]{"049", "900", "901", "906", "907", "910", "913", "914", "915", "920", "922", "925", "926", "930", "935", "938", "940", "945", "948", "950", "951", "955", "961", "963", "970", "971", "987", "989", "990", "991", "992", "993", "994", "995", "996", "997", "998", "999"};
				List<VariableField> fieldsToRemove = record.getVariableFields(fieldNumbersToRemove);
				for (VariableField curField : fieldsToRemove){
					record.removeVariableField(curField);
				}

				//Add 913
				DataField field913 = new DataFieldImpl("913", ' ', ' ');
				field913.addSubfield(new SubfieldImpl('a', "LV " + curDate));
				record.addVariableField(field913);

				//Move 001 to 914 only if it does not start with ocm
				ControlField field001 = (ControlField)record.getVariableField("001");
				if (field001 != null){
					lastRecordId = field001.getData();
					if (!field001.getData().matches("(ocm|ocn|on|sky|in).*")){
						DataField field914 = new DataFieldImpl("914", ' ', ' ');
						field914.addSubfield(new SubfieldImpl('a', field001.getData()));
						record.addVariableField(field914);
						record.removeVariableField(field001);
					}else{
						String normalizedControlNumber = field001.getData();
						normalizedControlNumber = normalizedControlNumber.replaceAll("ocm|ocn|on|sky|in", "");
						field001.setData(normalizedControlNumber);
					}
				}else{
					lastRecordId = "no control number";
				}

				//Alter item fields
				Record specialRecordForReload = MarcFactoryImpl.newInstance().newRecord(record.getLeader());
				List<ControlField> controlFieldsToCopy = (List<ControlField>)record.getControlFields();
				for (ControlField controlField : controlFieldsToCopy){
					specialRecordForReload.addVariableField(controlField);
				}
				List<DataField> dataFieldsToCopy = (List<DataField>)record.getDataFields();
				for (DataField dataField : dataFieldsToCopy){
					if (!dataField.getTag().equals("949")) {
						specialRecordForReload.addVariableField(dataField);
					}
				}
				List<DataField> itemFields = record.getVariableFields("949");
				for (DataField itemField : itemFields){
					boolean suppressItem = false;
					boolean hasItemNotesField = false;
					String itemNotes = "";
					//Move to 949 field
					itemField.setTag("949");

					Subfield subfield_i = itemField.getSubfield('i');
					Subfield barcode_subfield = itemField.getSubfield('n');
					if (barcode_subfield != null){
						barcode_subfield.setCode('i');
					}
					if (subfield_i != null){
						subfield_i.setCode('n');
						hasItemNotesField = true;
						itemNotes = subfield_i.getData();
					}
					Subfield subfield_o = itemField.getSubfield('o');
					if (subfield_o != null){
						//Create a note field, and move subfield o to subfield f
						Subfield num_checkouts_subfield = new SubfieldImpl('n', "Millennium_CKOs_" + subfield_o.getData());
						itemField.addSubfield(num_checkouts_subfield);
						subfield_o.setCode('1');
					}
					Subfield last_checkout_subfield = itemField.getSubfield('y');
					if (last_checkout_subfield != null){
						last_checkout_subfield.setData("Last_Millennium_CKO_date_" + formatHorizonDate(last_checkout_subfield.getData()));
						last_checkout_subfield.setCode('n');
					}
					Subfield creation_date_subfield = itemField.getSubfield('k');
					if (creation_date_subfield != null){
						creation_date_subfield.setData("Millennium_Creation_Date_" + formatHorizonDate(creation_date_subfield.getData()));
						creation_date_subfield.setCode('n');
					}
					//Not sure if this will come from Millennium
					Subfield source_subfield = itemField.getSubfield('s');
					if (source_subfield != null){
						source_subfield.setData("Source_" + source_subfield.getData());
						source_subfield.setCode('n');
					}
					Subfield subfield_p = itemField.getSubfield('p');
					if (subfield_p != null && subfield_p.getData().equals("0")){
						itemField.removeSubfield(subfield_p);
					}
					Subfield callnumber_subfield = itemField.getSubfield('d');
					String callNumber = "";
					if (callnumber_subfield != null){
						callnumber_subfield.setCode('a');
						callNumber = callnumber_subfield.getData();
					}

					Subfield subfield_z = itemField.getSubfield('z');
					if (subfield_z == null){
						//Check the call number to see if we can figure out what type of call number it is.
						if (callNumber.matches("\\d+\\.?\\d+.*")){
							subfield_z = new SubfieldImpl('z', "092");
							itemField.addSubfield(subfield_z);
						}else{
							subfield_z = new SubfieldImpl('z', "099");
							itemField.addSubfield(subfield_z);
						}

					}else{
						String subfield_z_data = subfield_z.getData().trim();
						if (subfield_z_data.equalsIgnoreCase("loc") || subfield_z_data.equalsIgnoreCase("gencir")){
							subfield_z.setData("099");
						}else if (subfield_z_data.matches(".*ddc")){
							subfield_z.setData("092");
						}else if (subfield_z_data.equalsIgnoreCase("lc")){
							subfield_z.setData("090");
							recordsWithLcCallNumbers.add(field001.getData());
						}else{
							subfield_z.setData("099");
							if (!unknownCallNumberTypes.contains(subfield_z_data)){
								unknownCallNumberTypes.add(subfield_z_data);
								logger.warn("Unknown call number type " + subfield_z_data);
							}
						}
					}

					Subfield status_subfield = itemField.getSubfield('v');
					String locationOverrideByStatus = null;
					boolean isSpecialStatus = false;
					String newStatus = "-";
					if (status_subfield != null){
						String subfield_s_data = status_subfield.getData().trim();
						status_subfield.setCode('s');
						newStatus = statusMap.get(subfield_s_data);
						if (newStatus != null && !newStatus.equals("None")){
							status_subfield.setData(newStatus);
							if (newStatus.equals("n")){
								billedItems.add(barcode_subfield.getData());
							}else if (newStatus.equals("$")){
								lostAndPaidItems.add(barcode_subfield.getData());
							}else if (newStatus.equals("z")){
								claimsReturnedItems.add(barcode_subfield.getData());
							}else if (!newStatus.equals("-")){
								isSpecialStatus = true;
							}
						}
					}

					Subfield location_subfield = itemField.getSubfield('m');
					if (location_subfield != null){
						//agency subfield
						Subfield subfield_h = new SubfieldImpl('h', "5");
						itemField.addSubfield(subfield_h);
						itemField.removeSubfield(location_subfield);
					}
					Subfield copy_subfield= itemField.getSubfield('e');
					if (copy_subfield != null){
						String subfield_g_data = copy_subfield.getData();
						copy_subfield.setCode('g');
						//Retain the subfield if numeric
						if (!subfield_g_data.matches("\\d{1,3}")){
							//Not numeric
							Pattern copyVolumePattern = Pattern.compile("^c\\.(\\d+) (pt\\.\\d+)$", Pattern.CANON_EQ | Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
							Matcher copyVolumeMatcher = copyVolumePattern.matcher(subfield_g_data);
							if (copyVolumeMatcher.find()){
								String copy = copyVolumeMatcher.group(1);
								String volume = copyVolumeMatcher.group(2);
								copy_subfield.setData(copy);
								//Add a volume subfield
								Subfield subfield_c = new SubfieldImpl('c', volume);
								itemField.addSubfield(subfield_c);
							}else{
								//Determine if we should filter the copy field to retain only the first character or two
								Pattern retainPartOfCopyPattern = Pattern.compile("^(\\d{1,2})[\\s+(-]*(vhs|pb|pbk|pbrbk|hrbk|prbk|os|kit|lp|cass|cd|cds|dvd|bb|paper|hrdbk|plwy|playaway|hdbk|loose-leaf|pk|binder|spiral|suppl\\.|looseleaf|bk\\.\\s\\+\\sCD|`|box|c|cd-rom|index|jdvd|mp3|mp3 cd|reference|rob|score)([`,.)]| \\(set of 3 DVDs\\)|\\ssuppl.|j| \\(pbk\\.\\))*$", Pattern.CANON_EQ | Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
								Matcher retainPartOfCopyMatcher = retainPartOfCopyPattern.matcher(subfield_g_data);
								if (retainPartOfCopyMatcher.find()){
									//Retain just the numeric part of the copy data
									String copy = retainPartOfCopyMatcher.group(1);
									copy_subfield.setData(copy);
								} else{
									//do a final validation to make sure the data looks like a volume
									Pattern isVolumePattern = Pattern.compile("^.*((v[\\s\\d]|v\\.|vol|vol\\.|ed\\.|pt\\.|bk\\.|c\\.|disc|no\\.|part|season|issue|seas\\.|suppl\\.|ttl\\.|index|casebk|level)\\s?[\\dIV-]+.*|vhs.+|\\d{4}|\\d{1,2}/\\d{1,2}/\\d{2,4}|\\w{3}(-|\\. )\\d{2,4}|\\d{1,2}/\\d{1,2}/\\d{2}-\\d{1,2}/\\d{1,2}/\\d{2}|\\d{1,2}/\\d{4}|\\d{4}[/-]\\d{4}|.*annual.*|\\d{4}\\s\\(pbk\\.\\)|\\d{1,2}(th|rd|nd|st)\\sed\\.|(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|spr|sum|fall|wint|golf|nfl|intro|study guide|nba).*|(Animal Wonders|Apes|Bears|Big Cats|Birds of prey|Deer, Moose & Elk|Eagles|Endangered Animals|Giraffes|Little cats|Otters, Skunks|Owls|Penguins|Polar Bears|Seals and Sea Lions|Sharks|Snakes|Spiders|Tigers|Turtles|Whales|Wild Dogs|Animal Champions|Koalas etc.|Wolves))$", Pattern.CANON_EQ | Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
									Matcher isVolumeMatcher = isVolumePattern.matcher(subfield_g_data);
									copy_subfield.setCode('c');
									if (!isVolumeMatcher.matches())  {
										//Probably not data we want, but we should retain the info so we don't lose data during conversion.
										//Log that it is unique though
										//itemField.removeSubfield(copy_subfield);
										uniqueCopyData.add(subfield_g_data);
									}
								}
							}
						}
					}

					Subfield subfield_t = itemField.getSubfield('t');
					if (subfield_t != null){
						String subfield_t_data = subfield_t.getData();
						String newIType = itemTypeMap.get(subfield_t_data.toLowerCase().trim());
						if (newIType != null && newIType.length() > 0 && !newIType.equals("-")){
							if (subfield_t_data.equals("ph") || subfield_t_data.equals("hf") || subfield_t_data.equals("bp") || subfield_t_data.equals("np")){
								forceUnique = true;
							}
							subfield_t.setData(newIType);
						}else{
							if (!unknownITypes.containsKey(subfield_t_data)){
								unknownITypes.put(subfield_t_data, 1L);
								logger.warn("Could not find a new iType for " + subfield_t_data);
							}else{
								unknownITypes.put(subfield_t_data, unknownITypes.get(subfield_t_data) + 1);
							}
							suppressItem = true;
						}

					}

					//Handle check-in note
					Subfield subfield_x = itemField.getSubfield('x');
					if (subfield_x != null){
						//Change the check-in note to have a subfield of 'm' for message
						subfield_x.setCode('m');
					}

					itemField.setIndicator2('1');

					if (suppressItem){
						record.removeVariableField(itemField);
					}else{
						if (isSpecialStatus || hasItemNotesField){
							hasSpecialStatusItems = true;
							if (hasItemNotesField) {
								specialStatusBarcodesWriter.write(itemNotes + " " + barcode_subfield.getData() + "\r\n");
							}else{
								specialStatusBarcodesWriter.write(barcode_subfield.getData() + "\r\n");
							}
							if (isSpecialStatus) {
								if (!barcodesWithSpecialStatuses.containsKey(newStatus)) {
									barcodesWithSpecialStatuses.put(newStatus, new TreeSet<>());
								}
								barcodesWithSpecialStatuses.get(newStatus).add(barcode_subfield.getData());
							}
							specialRecordForReload.addVariableField(itemField);
						}
					}
					//End processing items
				}


				//Search for a match (in marmot or in loaded records?)
				GroupedRecord recordToImport = frbrIzer.getGroupedRecordForMarc(record, false);
				//Check to see if the record is a perfect match
				ResultSet perfectMatchFromCatalog = frbrIzer.getGroupedRecordFromCatalog(recordToImport);
				boolean makeAllUnique = false;
				//Write to file
				if (!forceUnique && !makeAllUnique && perfectMatchFromCatalog.next()){
					/*if (matchingRecord.relatedBibNumbers.size() > 1){
						System.out.println("Matching to a record with " + matchingRecord.relatedBibNumbers.size() + " related bib numbers");
					} */
					DataField field907 = new DataFieldImpl("907", ' ', ' ');
					field907.addSubfield(new SubfieldImpl('a', perfectMatchFromCatalog.getString("primary_bib_number")));
					record.addVariableField(field907);
					specialRecordForReload.addVariableField(field907);
					try {
						matchedRecordWriter.write(record);
					}catch (Exception e){
						logger.error("Error writing bib for record " + numRecordsRead, e);
					}
					numMatchedBibs++;
				} else{
					//Not a perfect match, do a fuzzy match
					String fuzzyMatchFromCatalog = frbrIzer.getFuzzyMatchFromCatalog(recordToImport);
					if (!forceUnique && !makeAllUnique && fuzzyMatchFromCatalog != null){
						DataField field907 = new DataFieldImpl("907", ' ', ' ');
						field907.addSubfield(new SubfieldImpl('a', fuzzyMatchFromCatalog));
						record.addVariableField(field907);
						specialRecordForReload.addVariableField(field907);
						try {
							fuzzyMatchedRecordWriter.write(record);
						}catch (Exception e){
							logger.error("Error writing bib for record " + numRecordsRead, e);
						}
						numFuzzyMatchBibs++;
					}else{
						if (hasSpecialStatusItems){
							logger.warn("Special Status Bib Record is unique, may want to force it to attach. " + lastRecordId);
						}
						numUniqueBibs++;
						try {
							if (itemFields.size() > 500){
								if (field001 != null){
									System.out.println("Treating " + field001.getData() + " as a large record");
								} else {
									String title = ((DataField) record.getVariableField("245")).getSubfield('a').getData();
									System.out.println("Treating " + title + " as a large record");
								}
								uniqueLargeRecordWriter.write(record);
							}else {
								uniqueRecordWriter.write(record);
							}
						}catch (Exception e){
							logger.error("Error writing bib for record " + numRecordsRead, e);
						}
					}

				}
				if (hasSpecialStatusItems){
					//Remove an items without a special status
					specialStatusItemsWriter.write(specialRecordForReload);
				}
				perfectMatchFromCatalog.close();
			}
			matchedRecordWriter.close();
			uniqueRecordWriter.close();
			specialStatusItemsWriter.close();

			FileWriter uniqueCopiesWriter = new FileWriter(uniqueCopiesDestination);
			for (String copyData : uniqueCopyData){
				uniqueCopiesWriter.write(copyData + "\r\n");
			}
			uniqueCopiesWriter.flush();
			uniqueCopiesWriter.close();

			FileWriter recordsWithLcCallNumbersWriter = new FileWriter(recordsWithLcCallNumbersDestination);
			for (String recordWithLcCall : recordsWithLcCallNumbers){
				recordsWithLcCallNumbersWriter.write(recordWithLcCall + "\r\n");
			}
			recordsWithLcCallNumbersWriter.flush();
			recordsWithLcCallNumbersWriter.close();

			//Write invalid itypes, collections, etc with counts
			FileWriter iTypesNotConvertedWriter = new FileWriter(iTypesNotConverted);
			for (String iType : unknownITypes.keySet()){
				iTypesNotConvertedWriter.write(iType + "," + unknownITypes.get(iType) + "\r\n");
			}
			iTypesNotConvertedWriter.flush();
			iTypesNotConvertedWriter.close();

			FileWriter collectionsNotConvertedWriter = new FileWriter(collectionsNotConverted);
			for (String collection : unknownCollections.keySet()){
				collectionsNotConvertedWriter.write(collection + "," + unknownCollections.get(collection) + "\r\n");
			}
			collectionsNotConvertedWriter.flush();
			collectionsNotConvertedWriter.close();

			FileWriter statusesNotConvertedWriter = new FileWriter(statusesNotConverted);
			for (String status : unknownStatuses.keySet()){
				statusesNotConvertedWriter.write(status + "," + unknownStatuses.get(status) + "\r\n");
			}
			statusesNotConvertedWriter.flush();
			statusesNotConvertedWriter.close();

			FileWriter lostAndPaidItemsWriter = new FileWriter(lostAndPaidItemsFile);
			for (String itemBarcode : lostAndPaidItems){
				lostAndPaidItemsWriter.write(itemBarcode + "\r\n");
			}
			lostAndPaidItemsWriter.flush();
			lostAndPaidItemsWriter.close();

			FileWriter claimsReturnedItemsWriter = new FileWriter(claimsReturnedItemsFile);
			for (String itemBarcode : claimsReturnedItems){
				claimsReturnedItemsWriter.write(itemBarcode + "\r\n");
			}
			claimsReturnedItemsWriter.flush();
			claimsReturnedItemsWriter.close();

			FileWriter billedItemsWriter = new FileWriter(billedItemsFile);
			for (String itemBarcode : billedItems){
				billedItemsWriter.write(itemBarcode + "\r\n");
			}
			billedItemsWriter.flush();
			billedItemsWriter.close();

			FileWriter invalidRecordWriter = new FileWriter(invalidRecordsFile);
			for (String invalidRecord : invalidRecords){
				invalidRecordWriter.write(invalidRecord + "\r\n");
			}
			invalidRecordWriter.flush();
			invalidRecordWriter.close();

			for (String specialStatus : barcodesWithSpecialStatuses.keySet()){
				FileWriter specialStatusWriter = new FileWriter(new File("C:\\web\\loveland_marc_conversion\\results\\barcodes_for_" + specialStatus + ".csv"));
				for (String barcode : barcodesWithSpecialStatuses.get(specialStatus)){
					specialStatusWriter.write(barcode + "\r\n");
				}
				specialStatusWriter.flush();
				specialStatusWriter.close();
			}

			logger.warn("Read " + numRecordsRead + " records from loveland extract");
			logger.warn("Removed " + numProjectGutenberg + " records from Project Gutenberg");
			logger.warn("Removed " + numOverdrive + " records from OverDrive");
			logger.warn("Processed " + numUniqueBibs + " unique bibs, " + numMatchedBibs + " perfect matches, and " + numFuzzyMatchBibs + " fuzzy matches.");
		}catch (Exception e){
			logger.error("Error processing marc records ", e);
			e.printStackTrace();
		}

		try{
			dbConnection.close();
		}catch (Exception e){
			logger.error("Error closing database ", e);
			System.exit(1);
		}
		logger.info("Finished conversion " + new Date().toString());
	}

	private static SimpleDateFormat dateFormatter = new SimpleDateFormat("MM-dd-yyyy");
	private static String formatHorizonDate(String originalDate){
		int numElapsedDays = Integer.parseInt(originalDate);
		GregorianCalendar startDate = new GregorianCalendar();
		startDate.set(1970, 1, 1);
		startDate.add(Calendar.DAY_OF_MONTH, numElapsedDays);
		return dateFormatter.format(startDate.getTime());
	}

	private static HashMap<String, String> itemTypeMap = new HashMap<>();
	private static void loadItemTypeMap(){
		itemTypeMap.put("b","0");
		itemTypeMap.put("bab","0");
		itemTypeMap.put("bb","0");
		itemTypeMap.put("bda","19");
		itemTypeMap.put("bdu","19");
		itemTypeMap.put("bg","5");
		itemTypeMap.put("bp","3");
		itemTypeMap.put("bta","10");
		itemTypeMap.put("cbd","19");
		itemTypeMap.put("ccb","0");
		itemTypeMap.put("cf","0");
		itemTypeMap.put("ci","");
		itemTypeMap.put("ckd","5");
		itemTypeMap.put("clk","5");
		itemTypeMap.put("cmd","11");
		itemTypeMap.put("cnf","0");
		itemTypeMap.put("cpb","1");
		itemTypeMap.put("cspp","25");
		itemTypeMap.put("ct","10");
		itemTypeMap.put("dvd","9");
		itemTypeMap.put("dvdc","9");
		itemTypeMap.put("dvdl","24");
		itemTypeMap.put("eb","-");
		itemTypeMap.put("ebr","13");
		itemTypeMap.put("el","0");
		itemTypeMap.put("enf","0");
		itemTypeMap.put("eum","13");
		itemTypeMap.put("fn","13");
		itemTypeMap.put("hf","2");
		itemTypeMap.put("ill","-");
		itemTypeMap.put("lim","");
		itemTypeMap.put("llt","5");
		itemTypeMap.put("md","11");
		itemTypeMap.put("mm","1");
		itemTypeMap.put("np","3");
		itemTypeMap.put("oo","0");
		itemTypeMap.put("ph","15");
		itemTypeMap.put("pi","0");
		itemTypeMap.put("qpb","1");
		itemTypeMap.put("r","15");
		itemTypeMap.put("re","0");
		itemTypeMap.put("sbb","0");
		itemTypeMap.put("scbd","19");
		itemTypeMap.put("spk","5");
		itemTypeMap.put("ss","5");
		itemTypeMap.put("stg","5");
		itemTypeMap.put("tcb","0");
		itemTypeMap.put("tech","13");
		itemTypeMap.put("tf","0");
		itemTypeMap.put("tnf","0");
		itemTypeMap.put("tpb","1");
		itemTypeMap.put("tr","1");
		itemTypeMap.put("unk","25?");
		itemTypeMap.put("yld","");
		itemTypeMap.put("yldd","");

	}

	private static HashMap<String, String> statusMap = new HashMap<>();
	private static void loadStatusMap(){
		statusMap.put("dbc","-");
		statusMap.put("dc","-");
		statusMap.put("dcb","-");
		statusMap.put("dcnb","-");
		statusMap.put("dcnf","-");
		statusMap.put("dcnl","-");
		statusMap.put("dcs","-");
		statusMap.put("dd","-");
		statusMap.put("dfic","-");
		statusMap.put("dl","-");
		statusMap.put("dluck","-");
		statusMap.put("dnf","-");
		statusMap.put("dnm","-");
		statusMap.put("dnn","-");
		statusMap.put("dnt","-");
		statusMap.put("dpar","-");
		statusMap.put("drumpus","-");
		statusMap.put("dsc","-");
		statusMap.put("dsp","-");
		statusMap.put("dtn","-");
		statusMap.put("dts","-");
		statusMap.put("dvp","-");
		statusMap.put("i","-");
		statusMap.put("o","-");
		statusMap.put("s","-");
		statusMap.put("ufa","-");
		statusMap.put("e","!");
		statusMap.put("h","!");
		statusMap.put("hc","!");
		statusMap.put("m","$");
		statusMap.put("br","d");
		statusMap.put("dmg","d");
		statusMap.put("me","d");
		statusMap.put("ps","d");
		statusMap.put("cbs","g");
		statusMap.put("mds","g");
		statusMap.put("pis","g");
		statusMap.put("bi","i");
		statusMap.put("da","j");
		statusMap.put("l","l");
		statusMap.put("lr","l");
		statusMap.put("trace","m");
		statusMap.put("a","None");
		statusMap.put("b","None");
		statusMap.put("ckod","None");
		statusMap.put("csa","None");
		statusMap.put("mi","None");
		statusMap.put("rb","None");
		statusMap.put("recall","None");
		statusMap.put("rw","None");
		statusMap.put("t","None");
		statusMap.put("tc","None");
		statusMap.put("th","None");
		statusMap.put("tr","None");
		statusMap.put("ts","None");
		statusMap.put("bo","o");
		statusMap.put("nc","o");
		statusMap.put("ask","p");
		statusMap.put("n","p");
		statusMap.put("rc","p");
		statusMap.put("r","r");
		statusMap.put("c","z");
	}

	private static HashMap<String, String> collectionToLocationMap = new HashMap<>();
	private static void loadCollectionToLocationMap(){
		collectionToLocationMap.put("ab","lanba");
		collectionToLocationMap.put("fa-i","laaaa");
		collectionToLocationMap.put("cbd","laacj");
		collectionToLocationMap.put("bdf","laafa");
		collectionToLocationMap.put("bdn","laana");
		collectionToLocationMap.put("arc","laara");
		collectionToLocationMap.put("bab","labaj");
		collectionToLocationMap.put("bb","labbj");
		collectionToLocationMap.put("enf","labij");
		collectionToLocationMap.put("bu","labua");
		collectionToLocationMap.put("car","lacaa");
		collectionToLocationMap.put("ccb","laccj");
		collectionToLocationMap.put("tcb","laccy");
		collectionToLocationMap.put("city","lacia");
		collectionToLocationMap.put("bc","lacoa");
		collectionToLocationMap.put("cspp","lacsa");
		collectionToLocationMap.put("ebr","ladba");
		collectionToLocationMap.put("ds","ladsa");
		collectionToLocationMap.put("sabd","laeaa");
		collectionToLocationMap.put("scbd","laeaj");
		collectionToLocationMap.put("sbb","laebj");
		collectionToLocationMap.put("sckd","laecj");
		collectionToLocationMap.put("saf","laefa");
		collectionToLocationMap.put("scf","laefj");
		collectionToLocationMap.put("spk","laekj");
		collectionToLocationMap.put("eum","laema");
		collectionToLocationMap.put("san","laena");
		collectionToLocationMap.put("scn","laenj");
		collectionToLocationMap.put("sm","laepa");
		collectionToLocationMap.put("spi","laepj");
		collectionToLocationMap.put("sra","laera");
		collectionToLocationMap.put("es","laesa");
		collectionToLocationMap.put("re","lafej");
		collectionToLocationMap.put("cf","lafgj");
		collectionToLocationMap.put("gf","lafga");
		collectionToLocationMap.put("tf","lafgy");
		collectionToLocationMap.put("m","lafma");
		collectionToLocationMap.put("cpb","lafpj");
		collectionToLocationMap.put("fpb","lafpa");
		collectionToLocationMap.put("tpb","lafpy");
		collectionToLocationMap.put("sf","lafsa");
		collectionToLocationMap.put("w","lafwa");
		collectionToLocationMap.put("cgn","lagnj");
		collectionToLocationMap.put("agn","lagna");
		collectionToLocationMap.put("tgn","lagny");
		collectionToLocationMap.put("ill","laill");
		collectionToLocationMap.put("bg","lakba");
		collectionToLocationMap.put("ckd","lakcj");
		collectionToLocationMap.put("clk","laklj");
		collectionToLocationMap.put("stg","lakpj");
		collectionToLocationMap.put("lpb","lalba");
		collectionToLocationMap.put("yld","lalda");
		collectionToLocationMap.put("lpf","lalfa");
		collectionToLocationMap.put("llt","lallj");
		collectionToLocationMap.put("lpm","lalma");
		collectionToLocationMap.put("lpn","lalna");
		collectionToLocationMap.put("cmd","lamcj");
		collectionToLocationMap.put("md","lamca");
		collectionToLocationMap.put("mpb","lampa");
		collectionToLocationMap.put("cb","lanbj");
		collectionToLocationMap.put("anf","lanfa");
		collectionToLocationMap.put("cnf","lanfj");
		collectionToLocationMap.put("tnf","lanfy");
		collectionToLocationMap.put("mm","laopa");
		collectionToLocationMap.put("par","lapaa");
		collectionToLocationMap.put("cp","lapca");
		collectionToLocationMap.put("per","lapea");
		collectionToLocationMap.put("tper","lapey");
		collectionToLocationMap.put("cper","lapej");
		collectionToLocationMap.put("pi","lapij");
		collectionToLocationMap.put("parp","lappa");
		collectionToLocationMap.put("pro","lapca");
		collectionToLocationMap.put("rd","larsa");
		collectionToLocationMap.put("ra","larfa");
		collectionToLocationMap.put("rpb","larpa");
		collectionToLocationMap.put("lps","lalsa");
		collectionToLocationMap.put("spb","laspa");
		collectionToLocationMap.put("ss","lassj");
		collectionToLocationMap.put("tablet","latba");
		collectionToLocationMap.put("dvdc","lavdj");
		collectionToLocationMap.put("dvdf","lavda");
		collectionToLocationMap.put("dvdl","lavla");
		collectionToLocationMap.put("dvdn","lavna");
		collectionToLocationMap.put("lpw","lalwa");
		collectionToLocationMap.put("wpb","lawpa");
		collectionToLocationMap.put("dcbn","lafxj");
		collectionToLocationMap.put("dcnf","lanxj");
		collectionToLocationMap.put("dfic","lanfx");
		collectionToLocationMap.put("dnn","lannx");
		collectionToLocationMap.put("dnm","lanmx");
		collectionToLocationMap.put("dsc","lanex");
		collectionToLocationMap.put("dnt","latxy");
		collectionToLocationMap.put("dpicture","lapxj");
		collectionToLocationMap.put("das","ladaa");
		collectionToLocationMap.put("dcs","ladcj");
		collectionToLocationMap.put("dnf","ladna");
		collectionToLocationMap.put("dlob","ladla");
		collectionToLocationMap.put("oo","laooa");
	}


}
