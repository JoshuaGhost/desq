package sandbox;

import java.io.FileInputStream;
import java.io.IOException;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.DictionaryIO;
import de.uni_mannheim.desq.dictionary.Item;
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
		DictionaryIO.saveToDel(System.out, restricted, true);
		
	}
	
	static void icdm16() throws IOException {
		// load the dictionary
		Dictionary dict = DictionaryIO.loadFromDel(
				new FileInputStream("data/icdm16/example-dict.del"), false);
		System.out.println(dict.allItems());
		
		DictionaryIO.saveToDel(System.out, dict, false);
	}
	
	
	public static void main(String[] args) throws IOException {
		nyt();
		//icdm16();
	}
}
