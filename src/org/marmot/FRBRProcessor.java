package org.marmot;

import org.apache.log4j.Logger;
import org.marc4j.marc.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

/**
 * Description goes here
 * Longmont Marc Conversion
 * User: Mark Noble
 * Date: 10/17/13
 * Time: 9:26 AM
 */
public class FRBRProcessor {
	private PreparedStatement getGroupedRecordStmt;
	private PreparedStatement insertGroupedRecordStmt;
	private PreparedStatement addBibToGroupedRecordStmt;
	private PreparedStatement getGroupedRecordIdentifiersStmt;
	private PreparedStatement addIdentifierToGroupedRecordStmt;
	private PreparedStatement getRecordsForIdentifierStmt;
	private Logger logger;

	public FRBRProcessor(Connection dbConnection, Logger logger) {
		this.logger = logger;
		try{
			getGroupedRecordStmt = dbConnection.prepareStatement("SELECT id, primary_bib_number FROM grouped_record where (grouping_title = ? OR (grouping_title IS NULL AND ? IS NULL))" +
					" AND (grouping_subtitle = ? OR (grouping_subtitle IS NULL AND ? IS NULL))" +
					" AND (grouping_author=? OR (grouping_author IS NULL AND ? IS NULL))" +
					" and (publisher = ? OR (publisher IS NULL AND ? IS NULL))" +
					" and (format = ? OR (format IS NULL AND ? IS NULL))" +
					" and (edition = ? OR (edition IS NULL AND ? IS NULL))");
			insertGroupedRecordStmt = dbConnection.prepareStatement("INSERT INTO grouped_record (grouping_title, grouping_subtitle, grouping_author, publisher, format, edition, primary_bib_number) VALUES (?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
			addBibToGroupedRecordStmt = dbConnection.prepareStatement("INSERT INTO grouped_record_related_bibs (grouped_record_id, bib_number, num_items, is_oclc_bib) VALUES (?, ?, ?, ?)");
			getGroupedRecordIdentifiersStmt = dbConnection.prepareStatement("SELECT * FROM grouped_record_identifiers WHERE grouped_record_id=?");
			addIdentifierToGroupedRecordStmt = dbConnection.prepareStatement("INSERT INTO grouped_record_identifiers (grouped_record_id, type, identifier) VALUES (?, ?, ?)");
			getRecordsForIdentifierStmt = dbConnection.prepareStatement("SELECT * FROM grouped_record INNER JOIN grouped_record_identifiers ON grouped_record.id = grouped_record_id WHERE type = ? and identifier = ? and format = ?");
		}catch (Exception e){
			logger.error("Error setting up prepared statements", e);
		}
	}

	public GroupedRecord getGroupedRecordForMarc(Record marcRecord, boolean recordFromMainCatalog){
		//Get data for the grouped record

		//Title
		DataField field245 = (DataField)marcRecord.getVariableField("245");
		String groupingTitle = null;
		String groupingSubtitle = null;
		String fullTitle = null;
		if (field245 != null && field245.getSubfield('a') != null){
			fullTitle = field245.getSubfield('a').getData();
			char nonFilingCharacters = field245.getIndicator2();
			if (nonFilingCharacters < '0' || nonFilingCharacters > '9') nonFilingCharacters = '0';
			int titleStart = Integer.parseInt(Character.toString(nonFilingCharacters));
			if (titleStart > 0 && titleStart < fullTitle.length()){
				groupingTitle = fullTitle.substring(titleStart);
			}else{
				groupingTitle = fullTitle;
			}

			//System.out.println(fullTitle);
			//Replace & with and for better matching
			groupingTitle = groupingTitle.replace("&", "and");
			groupingTitle = groupingTitle.replaceAll("[^\\w\\d\\s]", "").toLowerCase();
			groupingTitle = groupingTitle.trim();

			//System.out.println(groupingTitle);
			int titleEnd = 100;
			if (titleEnd < groupingTitle.length()) {
				groupingTitle = groupingTitle.substring(0, titleEnd);
			}

			//Add in subtitle (subfield b as well to avoid problems with gov docs, etc)
			if (field245.getSubfield('b') != null){
				groupingSubtitle = field245.getSubfield('b').getData();
				groupingSubtitle = groupingSubtitle.replaceAll("&", "and");
				groupingSubtitle = groupingSubtitle.replaceAll("[^\\w\\d\\s]", "").toLowerCase();
				if (groupingSubtitle.length() > 175){
					groupingSubtitle = groupingSubtitle.substring(0, 175);
				}
				groupingSubtitle = groupingSubtitle.trim();
			}

		}
		//System.out.println(groupingTitle);

		//Author
		String author = null;
		DataField field100 = (DataField)marcRecord.getVariableField("100");
		if (field100 != null && field100.getSubfield('a') != null){
			author = field100.getSubfield('a').getData();
		}else{
			DataField field110 = (DataField)marcRecord.getVariableField("110");
			if (field110 != null && field110.getSubfield('a') != null){
				author = field110.getSubfield('a').getData();
			}
		}
		String groupingAuthor = null;
		if (author != null){
			groupingAuthor = author.replaceAll("[^\\w\\d\\s]", "").trim().toLowerCase();
			if (groupingAuthor.length() > 50){
				groupingAuthor = groupingAuthor.substring(0, 50);
			}
		}

		//Record Number
		String recordNumber = null;
		List<DataField> field907 = marcRecord.getVariableFields("907");
		for (DataField cur907 : field907){
			if (cur907.getSubfield('a').getData().matches("\\.b.*")){
				recordNumber = cur907.getSubfield('a').getData();
				break;
			}
		}

		//Format
		String format = getFormat(marcRecord);

		//Publisher
		String publisher = getPublisher(marcRecord);
		if (publisher != null){
			publisher = publisher.replaceAll("[^\\w\\d\\s]", "").trim().toLowerCase();
			if (publisher.length() > 50) {
				publisher = publisher.substring(0, 50);
			}
		}

		//Load edition
		DataField field250 = (DataField) marcRecord.getVariableField("250");
		String edition = null;
		if (field250 != null && field250.getSubfield('a') != null){
			edition = field250.getSubfield('a').getData().toLowerCase();
			edition = edition.replaceAll("[^\\w\\d\\s]", "").trim().toLowerCase();
			if (edition.length() > 50) {
				edition = edition.substring(0, 50);
			}
		}

		//Load identifiers
		HashSet<GroupedRecordIdentifier> identifiers = new HashSet<GroupedRecordIdentifier>();
		List<DataField> identifierFields = marcRecord.getVariableFields(new String[]{"020", "024", "022", "035"});
		for (DataField identifierField : identifierFields){
			if (identifierField.getSubfield('a') != null){
				String identifierValue = identifierField.getSubfield('a').getData().trim();
				//Get rid of any extra data at the end of the identifier
				if (identifierValue.indexOf(' ') > 0){
					identifierValue = identifierValue.substring(0, identifierValue.indexOf(' '));
				}
				String identifierType = null;
				if (identifierField.getTag().equals("020")){
					identifierType = "isbn";
					identifierValue = identifierValue.replaceAll("\\D", "");
					if (identifierValue.length() == 10){
						identifierValue = convertISBN10to13(identifierValue);
					}
				}else if (identifierField.getTag().equals("024")){
					identifierType = "upc";
				}else if (identifierField.getTag().equals("022")){
					identifierType = "issn";
				}else {
					identifierType = "oclc";
					identifierValue = identifierValue.replaceAll("\\(.*\\)", "");
				}
				GroupedRecordIdentifier identifier = new GroupedRecordIdentifier();
				if (identifierValue.length() > 20){
					//System.out.println("Found long identifier " + identifierType + " " + identifierValue + " skipping");
					continue;
				}else if (identifierValue.length() == 0){
					continue;
				}
				identifier.identifier = identifierValue;
				identifier.type = identifierType;
				identifiers.add(identifier);
			}
		}

		//Check to see if the record is an oclc number
		ControlField field001 = marcRecord.getControlNumberField();
		boolean isOclcNumber = false;
		if (field001 != null && field001.getData().matches("^(ocm|oc|om).*")){
			isOclcNumber = true;
		}

		//Get number of items
		List<VariableField> itemFields = marcRecord.getVariableFields("989");
		int numItems = itemFields.size();

		GroupedRecord workForTitle = new GroupedRecord();
		workForTitle.groupingTitle = groupingTitle;
		workForTitle.groupingSubtitle = groupingSubtitle;
		workForTitle.groupingAuthor = groupingAuthor;
		workForTitle.bibNumber = recordNumber;
		workForTitle.publisher = publisher;
		workForTitle.format = format;
		workForTitle.edition = edition;
		workForTitle.isOclcBib = isOclcNumber;
		workForTitle.numItems = numItems;
		workForTitle.identifiers = identifiers;

		return workForTitle;
	}

	private void updatePotentialMatches(List<GroupedRecord> potentialMatches, List<GroupedRecord> matchingRecordsByFactor) {
		//Check each potential match and remove if it is not in the matching records by factor list
		for (int i = potentialMatches.size() -1; i >= 0; i--){
			GroupedRecord potentialMatch = potentialMatches.get(i);
			if (!matchingRecordsByFactor.contains(potentialMatch)){
				potentialMatches.remove(i);
			}
		}
	}

	public ResultSet getGroupedRecordFromCatalog(GroupedRecord originalRecord){
		try{
			getGroupedRecordStmt.setString(1, originalRecord.groupingTitle);
			getGroupedRecordStmt.setString(2, originalRecord.groupingTitle);
			getGroupedRecordStmt.setString(3, originalRecord.groupingSubtitle);
			getGroupedRecordStmt.setString(4, originalRecord.groupingSubtitle);
			getGroupedRecordStmt.setString(5, originalRecord.groupingAuthor);
			getGroupedRecordStmt.setString(6, originalRecord.groupingAuthor);
			getGroupedRecordStmt.setString(7, originalRecord.publisher);
			getGroupedRecordStmt.setString(8, originalRecord.publisher);
			getGroupedRecordStmt.setString(9, originalRecord.format);
			getGroupedRecordStmt.setString(10, originalRecord.format);
			getGroupedRecordStmt.setString(11, originalRecord.edition);
			getGroupedRecordStmt.setString(12, originalRecord.edition);
			ResultSet potentialMatches = getGroupedRecordStmt.executeQuery();
			return potentialMatches;
		}catch (Exception e){
			logger.error("Error adding record to works ", e);
		}
		return null;
	}

	public String getFuzzyMatchFromCatalog(GroupedRecord originalRecord){
		//Get a fuzzy match from the catalog.
		//Check identifiers
		for (GroupedRecordIdentifier recordIdentifier : originalRecord.identifiers){
			try{
				getRecordsForIdentifierStmt.setString(1, recordIdentifier.type);
				getRecordsForIdentifierStmt.setString(2, recordIdentifier.identifier);
				getRecordsForIdentifierStmt.setString(3, originalRecord.format);
				ResultSet recordsForIdentifier = getRecordsForIdentifierStmt.executeQuery();
				//Check to see how many matches we got.
				if (!recordsForIdentifier.next()){
					//No matches, keep going to the next identifier
				}else{
					recordsForIdentifier.last();
					int numMatches = recordsForIdentifier.getRow();
					if (numMatches == 1){
						//We got a good match!
						String matchingBib = recordsForIdentifier.getString("primary_bib_number");
						String title = recordsForIdentifier.getString("grouping_title");
						String author = recordsForIdentifier.getString("grouping_author");
						if (!compareStrings(title, originalRecord.groupingTitle)){
							logger.info("Found match by identifier, but title did not match.  Sierra bib " + matchingBib);
							logger.info("  '" + title + "' != '" + originalRecord.groupingTitle + "'");
						} else if (!compareStrings(author, originalRecord.groupingAuthor)){
							logger.info("Found match by identifier, but author did not match.  Sierra bib " + matchingBib);
							logger.info("  '" + author + "' != '" + originalRecord.groupingAuthor + "'");
						}else{
							//This seems to be a good match
							return matchingBib;
						}
					}else{
						//Hmm, there are multiple records based on ISBN.  Check more stuff
						logger.info("Multiple grouped records found for identifier " + recordIdentifier.type + " " + recordIdentifier.identifier + " found " + numMatches);
					}
				}
			}catch (Exception e){
				logger.error("Error loading records for identifier " + recordIdentifier.type + " " + recordIdentifier.identifier, e);
			}
		}
		return null;
	}

	public void addRecordToWorks(Record marcRecord){
		GroupedRecord groupedRecord = getGroupedRecordForMarc(marcRecord, true);

		//Check to see if there is already a grouped record in the database
		try{
			ResultSet potentialMatches = getGroupedRecordFromCatalog(groupedRecord);
			long groupedRecordId = -1;
			if (potentialMatches.next()){
				groupedRecordId = potentialMatches.getLong("id");
			} else {
				//No match, add a record
				insertGroupedRecordStmt.setString(1, groupedRecord.groupingTitle);
				insertGroupedRecordStmt.setString(2, groupedRecord.groupingSubtitle);
				insertGroupedRecordStmt.setString(3, groupedRecord.groupingAuthor);
				insertGroupedRecordStmt.setString(4, groupedRecord.publisher);
				insertGroupedRecordStmt.setString(5, groupedRecord.format);
				insertGroupedRecordStmt.setString(6, groupedRecord.edition);
				insertGroupedRecordStmt.setString(7, groupedRecord.bibNumber);

				insertGroupedRecordStmt.executeUpdate();
				ResultSet generatedKeysRS = insertGroupedRecordStmt.getGeneratedKeys();
				if (generatedKeysRS.next()){
					groupedRecordId = generatedKeysRS.getLong(1);
				}
			}
			//Add the related bib
			addBibToGroupedRecordStmt.setLong(1, groupedRecordId);
			addBibToGroupedRecordStmt.setString(2, groupedRecord.bibNumber);
			addBibToGroupedRecordStmt.setLong(3, groupedRecord.numItems);
			addBibToGroupedRecordStmt.setBoolean(4, groupedRecord.isOclcBib);
			addBibToGroupedRecordStmt.executeUpdate();

			//Update identifiers
			getGroupedRecordIdentifiersStmt.setLong(1, groupedRecordId);
			ResultSet existingIdentifiersRS = getGroupedRecordIdentifiersStmt.executeQuery();
			HashSet<GroupedRecordIdentifier> existingIdentifiers = new HashSet<GroupedRecordIdentifier>();
			while (existingIdentifiersRS.next()){
				GroupedRecordIdentifier existingIdentifier = new GroupedRecordIdentifier();
				existingIdentifier.type = existingIdentifiersRS.getString("type");
				existingIdentifier.identifier = existingIdentifiersRS.getString("identifier");
				existingIdentifiers.add(existingIdentifier);
			}

			for (GroupedRecordIdentifier curIdentifier :  groupedRecord.identifiers){
				if (!existingIdentifiers.contains(curIdentifier)){
					addIdentifierToGroupedRecordStmt.setLong(1, groupedRecordId);
					addIdentifierToGroupedRecordStmt.setString(2, curIdentifier.type);
					addIdentifierToGroupedRecordStmt.setString(3, curIdentifier.identifier);
					addIdentifierToGroupedRecordStmt.executeUpdate();
				}
			}
		}catch (Exception e){
			logger.error("Error adding record to works ", e);
		}
	}

	private String getFormat(Record record) {
		//Check to see if the title is eContent based on the 989 field
		List<DataField> itemFields = record.getVariableFields("989");
		for (DataField itemField : itemFields){
			if (itemField.getSubfield('w') != null){
				//The record is some type of eContent.  For this purpose, we don't care what type
				return "eContent";
			}
		}

		String leader = record.getLeader().toString();
		char leaderBit;
		ControlField fixedField = (ControlField) record.getVariableField("008");
		//DataField title = (DataField) record.getVariableField("245");
		char formatCode;

		// check for music recordings quickly so we can figure out if it is music
		// for category (need to do here since checking what is on the Compact
		// Disc/Phonograph, etc is difficult).
		if (leader.length() >= 6) {
			leaderBit = leader.charAt(6);
			switch (Character.toUpperCase(leaderBit)) {
				case 'J':
					return "MusicRecording";
			}
		}

		// check for playaway in 260|b
		DataField sysDetailsNote = (DataField) record.getVariableField("260");
		if (sysDetailsNote != null) {
			if (sysDetailsNote.getSubfield('b') != null) {
				String sysDetailsValue = sysDetailsNote.getSubfield('b').getData().toLowerCase();
				if (sysDetailsValue.contains("playaway")) {
					return "Playaway";
				}
			}
		}

		// Check for formats in the 538 field
		DataField sysDetailsNote2 = (DataField) record.getVariableField("538");
		if (sysDetailsNote2 != null) {
			if (sysDetailsNote2.getSubfield('a') != null) {
				String sysDetailsValue = sysDetailsNote2.getSubfield('a').getData().toLowerCase();
				if (sysDetailsValue.contains("playaway")) {
					return "Playaway";
				} else if (sysDetailsValue.contains("bluray")
						|| sysDetailsValue.contains("blu-ray")) {
					return "Blu-ray";
				} else if (sysDetailsValue.contains("dvd")) {
					return "DVD";
				} else if (sysDetailsValue.contains("vertical file")) {
					return "VerticalFile";
				}
			}
		}

		// Check for formats in the 500 tag
		DataField noteField = (DataField) record.getVariableField("500");
		if (noteField != null) {
			if (noteField.getSubfield('a') != null) {
				String noteValue = noteField.getSubfield('a').getData().toLowerCase();
				if (noteValue.contains("vertical file")) {
					return "VerticalFile";
				}
			}
		}

		// Check for large print book (large format in 650, 300, or 250 fields)
		// Check for blu-ray in 300 fields
		DataField edition = (DataField) record.getVariableField("250");
		if (edition != null) {
			if (edition.getSubfield('a') != null) {
				if (edition.getSubfield('a').getData().toLowerCase().contains("large type")) {
					return "LargePrint";
				}
			}
		}

		@SuppressWarnings("unchecked")
		List<DataField> physicalDescription = record.getVariableFields("300");
		if (physicalDescription != null) {
			Iterator<DataField> fieldsIter = physicalDescription.iterator();
			DataField field;
			while (fieldsIter.hasNext()) {
				field = fieldsIter.next();
				@SuppressWarnings("unchecked")
				List<Subfield> subFields = field.getSubfields();
				for (Subfield subfield : subFields) {
					if (subfield.getData().toLowerCase().contains("large type")) {
						return "LargePrint";
					} else if (subfield.getData().toLowerCase().contains("bluray")
							|| subfield.getData().toLowerCase().contains("blu-ray")) {
						return "Blu-ray";
					}
				}
			}
		}
		@SuppressWarnings("unchecked")
		List<DataField> topicalTerm = record.getVariableFields("650");
		if (topicalTerm != null) {
			Iterator<DataField> fieldsIter = topicalTerm.iterator();
			DataField field;
			while (fieldsIter.hasNext()) {
				field = fieldsIter.next();
				@SuppressWarnings("unchecked")
				List<Subfield> subfields = field.getSubfields();
				for (Subfield subfield : subfields) {
					if (subfield.getData().toLowerCase().contains("large type")) {
						return "LargePrint";
					}
				}
			}
		}

		@SuppressWarnings("unchecked")
		List<DataField> localTopicalTerm = record.getVariableFields("690");
		if (localTopicalTerm != null) {
			Iterator<DataField> fieldsIterator = localTopicalTerm.iterator();
			DataField field;
			while (fieldsIterator.hasNext()) {
				field = fieldsIterator.next();
				Subfield subfieldA = field.getSubfield('a');
				if (subfieldA != null) {
					if (subfieldA.getData().toLowerCase().contains("seed library")) {
						return "SeedPacket";
					}
				}
			}
		}

		// check the 007 - this is a repeating field
		@SuppressWarnings("unchecked")
		List<DataField> fields = record.getVariableFields("007");
		if (fields != null) {
			Iterator<DataField> fieldsIter = fields.iterator();
			ControlField formatField;
			while (fieldsIter.hasNext()) {
				formatField = (ControlField) fieldsIter.next();
				if (formatField.getData() == null || formatField.getData().length() < 2) {
					continue;
				}
				// Check for blu-ray (s in position 4)
				// This logic does not appear correct.
				/*
				 * if (formatField.getData() != null && formatField.getData().length()
				 * >= 4){ if (formatField.getData().toUpperCase().charAt(4) == 'S'){
				 * result.add("Blu-ray"); break; } }
				 */
				formatCode = formatField.getData().toUpperCase().charAt(0);
				switch (formatCode) {
					case 'A':
						switch (formatField.getData().toUpperCase().charAt(1)) {
							case 'D':
								return "Atlas";
							default:
								return "Map";
						}
					case 'C':
						switch (formatField.getData().toUpperCase().charAt(1)) {
							case 'A':
								return "TapeCartridge";
							case 'B':
								return "ChipCartridge";
							case 'C':
								return "DiscCartridge";
							case 'F':
								return "TapeCassette";
							case 'H':
								return "TapeReel";
							case 'J':
								return "FloppyDisk";
							case 'M':
							case 'O':
								return "CDROM";
							case 'R':
								// Do not return - this will cause anything with an
								// 856 field to be labeled as "Electronic"
								break;
							default:
								return "Software";
						}
						break;
					case 'D':
						return "Globe";
					case 'F':
						return "Braille";
					case 'G':
						switch (formatField.getData().toUpperCase().charAt(1)) {
							case 'C':
							case 'D':
								return "Filmstrip";
							case 'T':
								return "Transparency";
							default:
								return "Slide";
						}
					case 'H':
						return "Microfilm";
					case 'K':
						switch (formatField.getData().toUpperCase().charAt(1)) {
							case 'C':
								return "Collage";
							case 'D':
								return "Drawing";
							case 'E':
								return "Painting";
							case 'F':
								return "Print";
							case 'G':
								return "Photonegative";
							case 'J':
								return "Print";
							case 'L':
								return "Drawing";
							case 'O':
								return "FlashCard";
							case 'N':
								return "Chart";
							default:
								return "Photo";
						}
					case 'M':
						switch (formatField.getData().toUpperCase().charAt(1)) {
							case 'F':
								return "VideoCassette";
							case 'R':
								return "Filmstrip";
							default:
								return "MotionPicture";
						}
					case 'O':
						return "Kit";
					case 'Q':
						return "MusicalScore";
					case 'R':
						return "SensorImage";
					case 'S':
						switch (formatField.getData().toUpperCase().charAt(1)) {
							case 'D':
								if (formatField.getData().length() >= 4) {
									char speed = formatField.getData().toUpperCase().charAt(3);
									if (speed >= 'A' && speed <= 'E') {
										return "Phonograph";
									} else if (speed == 'F') {
										return "CompactDisc";
									} else if (speed >= 'K' && speed <= 'R') {
										return "TapeRecording";
									} else {
										return "SoundDisc";
									}
								} else {
									return "SoundDisc";
								}
							case 'S':
								return "SoundCassette";
							default:
								return "SoundRecording";
						}
					case 'T':
						switch (formatField.getData().toUpperCase().charAt(1)) {
							case 'A':
								return "Book";
							case 'B':
								return "LargePrint";
						}
					case 'V':
						switch (formatField.getData().toUpperCase().charAt(1)) {
							case 'C':
								return "VideoCartridge";
							case 'D':
								return "VideoDisc";
							case 'F':
								return "VideoCassette";
							case 'R':
								return "VideoReel";
							default:
								return "Video";
						}
				}
			}
		}

		// check the Leader at position 6
		if (leader.length() >= 6) {
			leaderBit = leader.charAt(6);
			switch (Character.toUpperCase(leaderBit)) {
				case 'C':
				case 'D':
					return "MusicalScore";
				case 'E':
				case 'F':
					return "Map";
				case 'G':
					// We appear to have a number of items without 007 tags marked as G's.
					// These seem to be Videos rather than Slides.
					// return "Slide");
					return "Video";
				case 'I':
					return "SoundRecording";
				case 'J':
					return "MusicRecording";
				case 'K':
					return "Photo";
				case 'M':
					return "Electronic";
				case 'O':
				case 'P':
					return "Kit";
				case 'R':
					return "PhysicalObject";
				case 'T':
					return "Manuscript";
			}
		}

		if (leader.length() >= 7) {
			// check the Leader at position 7
			leaderBit = leader.charAt(7);
			switch (Character.toUpperCase(leaderBit)) {
				// Monograph
				case 'M':
					return "Book";
				// Serial
				case 'S':
					// Look in 008 to determine what type of Continuing Resource
					if (fixedField != null){
						formatCode = fixedField.getData().toUpperCase().charAt(21);
						switch (formatCode) {
							case 'N':
								return "Newspaper";
							case 'P':
								return "Journal";
							default:
								return "Serial";
						}
					}
			}
		}

		// Nothing worked!
		return "Unknown";
	}

	private String getPublisher(Record marcRecord) {
		//First check for 264 fields
		@SuppressWarnings("unchecked")
		List<DataField> rdaFields = (List<DataField>)marcRecord.getVariableFields(new String[]{"264", "260"});
		if (rdaFields.size() > 0){
			for (DataField curField : rdaFields){
				if (curField.getIndicator2() == '1' || curField.getTag().equals("260")){
					Subfield subFieldB = curField.getSubfield('b');
					if (subFieldB != null){
						return subFieldB.getData();
					}
				}
			}
		}
		return null;
	}

	public String convertISBN10to13(String isbn10){
		if (isbn10.length() != 10){
			return null;
		}
		String isbn = "978" + isbn10.substring(0, 9);
		//Calculate the 13 digit checksum
		int sumOfDigits = 0;
		for (int i = 0; i < 12; i++){
			int multiplier = 1;
			if (i % 2 == 1){
				multiplier = 3;
			}
			sumOfDigits += multiplier * (int)(isbn.charAt(i));
		}
		int modValue = sumOfDigits % 10;
		int checksumDigit;
		if (modValue == 0){
			checksumDigit = 0;
		}else{
			checksumDigit = 10 - modValue;
		}
		return  isbn + Integer.toString(checksumDigit);
	}

	public boolean compareStrings(String catalogValue, String importValue) {
		if (catalogValue == null){
			//If we have a value in the import, but not the catalog that's ok.
			return true;
		}else if (importValue == null){
			//If we have a value in the catalog, but not the import, that's ok
			return true;
		}else{
			if (catalogValue.equals(importValue) || catalogValue.startsWith(importValue) || importValue.startsWith(catalogValue) || catalogValue.endsWith(importValue) || importValue.endsWith(catalogValue)){
				//Got a good match
				return true;
			}else{
				//Match without spaces since sometimes we get 1 2 3 in one catalog and 123 in another
				String catalogValueNoSpaces = catalogValue.replace(" ", "");
				String importValueNoSpaces = importValue.replace(" ", "");
				if (catalogValueNoSpaces.equals(importValueNoSpaces) || catalogValueNoSpaces.startsWith(importValueNoSpaces) || importValue.startsWith(catalogValue) || catalogValueNoSpaces.endsWith(importValueNoSpaces) || importValue.endsWith(catalogValue)){
					logger.debug("Matched string when spaces were removed.");
					//Got a good match
					return true;
				}
				return false;
			}
		}
	}
}
