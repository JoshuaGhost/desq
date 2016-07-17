package sandbox;

import java.io.FileInputStream;
import java.io.IOException;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.DictionaryIO;
import de.uni_mannheim.desq.dictionary.Item;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.DelSequenceWriter;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.io.SequenceWriter;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntSet;

public class DictionaryExample {
	static void nyt() throws IOException {
		// load the dictionary
		Dictionary dict = DictionaryIO.loadFromDel(
				new FileInputStream("data-local/nyt-1991-dict.del"), true);
		
		Item item;
		IntSet fids;

		// compute ascendants
		item = dict.getItemBySid("was@be@VB@");
		System.out.println(item);
		fids = dict.ascendantsFids(item.fid);
		System.out.println("Asc: " + dict.getItemsByFids(fids));

		// compute descendants
		item = dict.getItemBySid("be@VB@");
		System.out.println(item);
		fids = dict.descendantsFids(item.fid);
		System.out.println("Desc: " + dict.getItemsByFids(fids));

		// restrict the dictionary to specified subset
		Dictionary restricted = dict.restrictedCopy(
				dict.descendantsFids(dict.getItemBySid("DT@").fid));
		DictionaryIO.saveToDel(System.out, restricted, false, true);
	
		// clear the counts in the dictionary and recopute
		System.out.println("Recomputing counts");
		System.out.println(
				DictionaryIO.itemToDelLine(dict.getItemBySid("be@VB@"), true, true));
		dict.clearCounts();
		System.out.println(
				DictionaryIO.itemToDelLine(dict.getItemBySid("be@VB@"), true, true));
		SequenceReader dataReader = new DelSequenceReader(
				new FileInputStream("data-local/nyt-1991-data.del"), false);
		dict.incCounts(dataReader);
		System.out.println(
				DictionaryIO.itemToDelLine(dict.getItemBySid("be@VB@"), true, true));
	}
	
	static void icdm16() throws IOException {
		// load the dictionary
		Dictionary dict = DictionaryIO.loadFromDel(
				new FileInputStream("data/icdm16/example-dict.del"), false);
		System.out.println("All items: " + dict.allItems());
		
		// print data
		System.out.println("Input sequences:");
		SequenceReader dataReader = new DelSequenceReader(
				new FileInputStream("data/icdm16/example-data.del"), false);
		IntList inputSequence = new IntArrayList();
		while (dataReader.readAsIds(inputSequence)) {
			System.out.println(dict.getItemsByIds(inputSequence));
		}
		
		// update hierarchy
		dataReader = new DelSequenceReader(
				new FileInputStream("data/icdm16/example-data.del"), false);
		dict.incCounts(dataReader);
		System.out.println("Dictionary with counts: ");
		DictionaryIO.saveToDel(System.out, dict, false, true);
		
		// update fids
		System.out.println("Dictionary with new fids: ");
		dict.recomputeFids();
		DictionaryIO.saveToDel(System.out, dict, true, true);
		
		// show converted input sequences
		dataReader = new DelSequenceReader(
				new FileInputStream("data/icdm16/example-data.del"), false);
		SequenceWriter dataWriter = new DelSequenceWriter(System.out, false);
		while (dataReader.readAsIds(inputSequence)) {
			dict.idsToFids(inputSequence);
			dataWriter.write(inputSequence);
		}
	}
	
	
	public static void main(String[] args) throws IOException {
		//nyt();
		icdm16();
	}
}
