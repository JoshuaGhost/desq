package sandbox;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.DictionaryIO;
import de.uni_mannheim.desq.io.MemoryPatternWriter;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.mining.DesqMiner;
import de.uni_mannheim.desq.mining.DesqMinerContext;
import de.uni_mannheim.desq.mining.DfsMiner;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class DfsMiningExample {
	void icdm16() throws IOException {
		URL dictFile = getClass().getResource("/icdm16-example/dict.del");
		URL dataFile = getClass().getResource("/icdm16-example/data.del");

		// load the dictionary
		Dictionary dict = DictionaryIO.loadFromDel(dictFile.openStream(), false);

		// update hierarchy
		SequenceReader dataReader = new DelSequenceReader(dataFile.openStream(), false);
		dict.incCounts(dataReader);
		dict.recomputeFids();
		//DictionaryIO.saveToDel(System.out, dict, true, true);

		// print sequences
		System.out.println("Input sequences:");
		dataReader = new DelSequenceReader(dataFile.openStream(), false);
		dataReader.setDictionary(dict);
		IntList inputSequence = new IntArrayList();
		while (dataReader.readAsFids(inputSequence)) {
			System.out.println(dict.getItemsByFids(inputSequence));
		}
		
		// Perform mining 
		int sigma = 4;
		int gamma = 0;
		int lambda = 2;
		boolean generalize = true;
		
		System.out.println("\nPatterns (sigma=" + sigma + ", gamma="+gamma 
				+ ", lambda="+lambda + ", generalize="+generalize);
		dataReader = new DelSequenceReader(dataFile.openStream(), false);
		dataReader.setDictionary(dict);
		DesqMinerContext ctx = new DesqMinerContext();
		ctx.sigma = sigma;
		MemoryPatternWriter result = new MemoryPatternWriter();
		ctx.patternWriter = result;
		ctx.dict = dict;
		DesqMiner miner = new DfsMiner(ctx,gamma,lambda,generalize);
		miner.addInputSequences(dataReader);
		miner.mine();
		
		for (int i=0; i<result.size(); i++) {
			System.out.print(result.getFrequency(i));
			System.out.print(": ");
			System.out.println(dict.getItemsByFids(result.getPattern(i)));
		}
	}

	public static void main(String[] args) throws IOException {
		//nyt();
		new DfsMiningExample().icdm16();
	}

}
