package de.uni_mannheim.desq.mining;

import java.io.IOException;
import java.util.Properties;

import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.util.PropertiesUtils;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public abstract class DesqMiner {
	// if null, patterns are mined but not collected
	protected DesqMinerContext ctx;
	
	protected DesqMiner(DesqMinerContext ctx) {
		this.ctx = ctx;
	}
	
	/** Adds a new input sequence (composed of fids). The provided sequence must 
	 * not be buffered by this miner. */
	protected abstract void addInputSequence(IntList inputSequence);
	
	public void addInputSequences(SequenceReader in) throws IOException {
		IntList inputSequence = new IntArrayList();
		while (in.readAsFids(inputSequence)) {
			addInputSequence(inputSequence);
		}
	}

	/** Mines all added input sequences */
	public abstract void mine();

	public static String patternExpressionFor(int gamma, int lambda, boolean generalize) {
		String capturedItem = "(." + (generalize ? "^" : "") + ")";
		String patternExpression = capturedItem + "[.{0," + gamma + "}" + capturedItem + "]{0," + (lambda-1) + "}";
		return patternExpression;
	}
}
