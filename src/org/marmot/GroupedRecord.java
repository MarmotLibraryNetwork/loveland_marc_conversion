package org.marmot;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * A work is a title written by a particular author.  It is unique based on title and author.
 * Since titles and authors are not always entered identically, we check the first 25 characters
 * of the name after the non filing indicators.
 *
 * To check the author we compare each part of the name independently just in case something
 * is entered in reverse order.
 *
 * Longmont Marc Conversion
 * User: Mark Noble
 * Date: 10/17/13
 * Time: 8:55 AM
 */
public class GroupedRecord {
	public String groupingTitle;
	public String groupingAuthor;
	public String groupingSubtitle;
	public String edition;
	public String format;
	public String publisher;
	public String bibNumber;
	public int numItems;
	public boolean isOclcBib;
	public HashSet<GroupedRecordIdentifier> identifiers = new HashSet<GroupedRecordIdentifier>();
}
