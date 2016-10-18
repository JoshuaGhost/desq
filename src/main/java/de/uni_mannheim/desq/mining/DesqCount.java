package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.fst.*;
import de.uni_mannheim.desq.patex.PatEx;
import de.uni_mannheim.desq.util.DesqProperties;
import de.uni_mannheim.desq.util.PrimitiveUtils;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;

public final class DesqCount extends DesqMiner {
	private static final Logger logger = Logger.getLogger(DesqCount.class);

	// -- parameters for de.uni_mannheim.desq.old.mining ---------------------------------------------------------------------------------------

	/** Minimum support */
	final long sigma;

	/** The pattern expression used for de.uni_mannheim.desq.old.mining */
	final String patternExpression;

	/** Whether or not to use the f-list for pruning output sequences that are guaranteed to be infrequent. */
	final boolean useFlist;

	/** If true, input sequences that do not match the pattern expression are pruned */
	final boolean pruneIrrelevantInputs;

	/** If true, the two-pass algorithm for DesqDfs is used */
	final boolean useTwoPass;


	// -- helper variables --------------------------------------------------------------------------------------------

	/** Stores the final state transducer (one-pass) */
	final Fst fst;

	/** Stores the reverse final state transducer (two-pass) */
	final Fst reverseFst;

	/** Stores the largest fid of an item with frequency at least sigma. Used to quickly determine
	 * whether an item is frequent (if fid <= largestFrequentFid, the item is frequent */
	final int largestFrequentFid;

	/** Sequence id of the next input sequence */
	int inputId;

	// -- helper variables --------------------------------------------------------------------------------------------

	/** The input sequence currenlty processed */
	IntList inputSequence;

	/** The support of the current input sequence */
	long inputSupport;

	/** Stores all mined sequences along with their frequency. Each long is composed of a 32-bit integer storing the
	 * actual count and a 32-bit integer storing the input id of the last input sequence that produced this output.  */
	final Object2LongOpenHashMap<Sequence> outputSequences = new Object2LongOpenHashMap<>();

	/** Stores iterators over output item/next state pairs for reuse. Indexed by input position. */
	final ArrayList<Iterator<ItemState>> itemStateIterators = new ArrayList<>();

	/** Stores the part of the output sequence produced so far. */
	final Sequence prefix;

	// -- helper variables for pruning and twopass --------------------------------------------------------------------

	/** The DFA corresponding to the pattern expression. Accepts if the FST can reach a final state on an input */
	final ExtendedDfa edfa;

	/** Sequence of EDFA states for current input */
	final ArrayList<ExtendedDfaState> edfaStateSequence;

	/** Positions for which current input reached a final state  */
	final IntList finalPos;


	// -- construction/clearing ---------------------------------------------------------------------------------------

	public DesqCount(DesqMinerContext ctx) {
		super(ctx);
		this.sigma = ctx.conf.getLong("desq.mining.min.support");
		this.useFlist = ctx.conf.getBoolean("desq.mining.use.flist");
		this.pruneIrrelevantInputs = ctx.conf.getBoolean("desq.mining.prune.irrelevant.inputs");
		this.useTwoPass = ctx.conf.getBoolean("desq.mining.use.two.pass");

		this.largestFrequentFid = ctx.dict.getLargestFidAboveDfreq(sigma);
		this.inputId = 0;
		
		patternExpression = ctx.conf.getString("desq.mining.pattern.expression");
		PatEx p = new PatEx(patternExpression, ctx.dict);
		Fst tempFst = p.translate();
		tempFst.minimize(); //TODO: move to translate

		if (useTwoPass) { // two-pass
			// two-pass will always prune irrelevant input sequences, so notify the user when the corresponding
			// property is not set
			if (!pruneIrrelevantInputs) {
				logger.warn("property desq.de.uni_mannheim.desq.old.mining.prune.irrelevant.inputs=false will be ignored because " +
						"desq.mining.use.two.pass=true");
			}

			// construct the DFA for the FST (for the forward pass)
			this.edfa = new ExtendedDfa(tempFst, ctx.dict);

			// and then reverse the FST (which we now only need for the backward pass)
			tempFst.reverse(false); // here we need the reverse de.uni_mannheim.desq.old.fst
			fst = null;
			reverseFst = tempFst;

			// initialize helper variables for two-pass
			edfaStateSequence = new ArrayList<>();
			finalPos = new IntArrayList();
		} else { // one-pass
			// store the FST
			fst = tempFst;
			reverseFst = null;

			// if we prune irrelevant inputs, construct the DFS for the FST
			if (pruneIrrelevantInputs) {
				this.edfa = new ExtendedDfa(fst, ctx.dict);
			} else {
				this.edfa = null;
			}

			// invalidate helper variables for two-pass
			edfaStateSequence = null;
			finalPos  = null;
		}

		// initalize helper variable for FST simulation
		prefix = new Sequence();
		outputSequences.defaultReturnValue(-1L);
	}

	public static DesqProperties createConf(String patternExpression, long sigma) {
		DesqProperties conf = new DesqProperties();
		conf.setProperty("desq.mining.miner.class", DesqCount.class.getCanonicalName());
		conf.setProperty("desq.mining.min.support", sigma);
		conf.setProperty("desq.mining.pattern.expression", patternExpression);
		conf.setProperty("desq.mining.use.flist", true);
		conf.setProperty("desq.mining.prune.irrelevant.inputs", false);
		conf.setProperty("desq.mining.use.two.pass", false);
		return conf;
	}


	// -- processing input sequences ---------------------------------------------------------------------------------

	@Override
	protected void addInputSequence(IntList sequence, long support, boolean allowBuffering) {
		assert prefix.isEmpty(); // will be maintained by stepOnePass()
		this.inputSequence = sequence;
		this.inputSupport = support;

		// two-pass version of DesqCount
		if (useTwoPass) {
			// run the input sequence through the EDFA and compute the state sequences as well as the positions before
			// which a final FST state is reached
			if (edfa.isRelevant(sequence, 0, edfaStateSequence, finalPos)) {
				// we now know that the sequence is relevant; process it
				// look at all positions before which a final FST state can be reached
				for (final int pos : finalPos) {
					// for those positions, start with each possible final FST state and go backwards
					for (State fstFinalState : edfaStateSequence.get(pos).getFstFinalStates()) {
						stepTwoPass(pos-1, fstFinalState, 0);
					}
				}
				inputId++;
			}
			edfaStateSequence.clear();
			finalPos.clear();
			return;
		}

		// one-pass version of DesqCount
		if (!pruneIrrelevantInputs || edfa.isRelevant(sequence, 0, 0)) {
			stepOnePass(0, fst.getInitialState(), 0);
			inputId++;
		}
	}

	// -- mining ------------------------------------------------------------------------------------------------------

	@Override
	public void mine() {
		// by this time, the result is already stored in outputSequences. We only need to filter out the infrequent
		// ones.
		for(Object2LongMap.Entry<Sequence> entry : outputSequences.object2LongEntrySet()) {
			long value = entry.getLongValue();
			int support = PrimitiveUtils.getLeft(value);
			if (support >= sigma) {
				if (ctx.patternWriter != null) {
					if (useTwoPass) {
						ctx.patternWriter.writeReverse(entry.getKey(), support);
					} else {
						ctx.patternWriter.write(entry.getKey(), support);
					}
				}
			}
		}
	}

	/** Produces all outputs of the given input sequence that would be generated by DesqCount. Note that if you
	 * use this method, all inputs added previously will be cleared. Moreover, the output might be in reverse order
	 * (determined by {@link #mine1reversedOutput()})
	 *
	 * @param inputSequence
	 * @return
	 */
	public ObjectSet<Sequence> mine1(IntList inputSequence, long inputSupport) {
		outputSequences.clear();
		addInputSequence(inputSequence, inputSupport, true);
		return outputSequences.keySet();
	}

	public boolean mine1reversedOutput() {
		return useTwoPass;
	}

	/** Simulates the FST starting from the given position and state. Maintains the invariant that the current
	 * output is stored in {@link #prefix}. Recursive version.
	 *
 	 * @param pos position of next input item
	 * @param state current state of FST
	 * @param level recursion level (used for reusing iterators without conflict)
	 */
	private void stepOnePass(int pos, State state, int level) {
		// if we reached a final state, we count the current sequence (if any)
		if(state.isFinal() && !prefix.isEmpty() && (!fst.getRequireFullMatch() || pos==inputSequence.size())) {
			countSequence(prefix);
		}

		// check if we already read the entire input
		if (pos == inputSequence.size()) {
			return;
		}

		// get iterator over next output item/state pairs; reuse existing ones if possible
		final int itemFid = inputSequence.getInt(pos); // the current input item
		Iterator<ItemState> itemStateIt;
		if(level >= itemStateIterators.size()) {
			itemStateIt = state.consume(itemFid);
			itemStateIterators.add(itemStateIt);
		} else {
			itemStateIt = state.consume(itemFid, itemStateIterators.get(level));
		}

		// iterate over output item/state pairs
        while(itemStateIt.hasNext()) {
            final ItemState itemState = itemStateIt.next();
			final int outputItemFid = itemState.itemFid;
			final State toState = itemState.state;

			if(outputItemFid == 0) { // EPS output
				// we did not get an output, so continue with the current prefix
				int newLevel = level + (itemStateIt.hasNext() ? 1 : 0); // no need to create new iterator if we are done on this level
				stepOnePass(pos + 1, toState, newLevel);
			} else {
				// we got an output; check whether it is relevant
				if (!useFlist || largestFrequentFid >= outputItemFid) {
					// now append this item to the prefix, continue running the FST, and remove the item once done
					prefix.add(outputItemFid);
					int newLevel = level + (itemStateIt.hasNext() ? 1 : 0); // no need to create new iterator if we are done on this level
					stepOnePass(pos + 1, toState, newLevel);
					prefix.removeInt(prefix.size() - 1);
				}
			}
		}
	}

	/** Simulates the reverse FST starting from the given position and state. Maintains the invariant that the current
	 * output is stored in {@link #prefix} in reverse order. Recursive version, used only for two-pass.
	 *
	 * @param pos position of next input item
	 * @param state current state of FST
	 * @param level recursion level (used for reusing iterators without conflict)
	 */
	private void stepTwoPass(int pos, State state, int level) {
		// check if we reached the beginning of the input sequence
		if(pos == -1) {
			// we consumed entire input in reverse -> we must have reached the inital state by two-pass correctness
			assert state.getId() == 0;
			if (!prefix.isEmpty()) {
				countSequence(prefix);
			}
			return;
		}

		// get iterator over next output item/state pairs; reuse existing ones if possible
		// note that the reverse FST is used here (since we process inputs backwards)
		// only iterates over states that we saw in the forward pass (the other ones can safely be skipped)
		final int itemFid = inputSequence.getInt(pos);
		Iterator<ItemState> itemStateIt;
		if (level>=itemStateIterators.size()) {
			itemStateIt = state.consume(itemFid, null, edfaStateSequence.get(pos).getFstStates());
			itemStateIterators.add(itemStateIt);
		} else {
			itemStateIt = state.consume(itemFid, itemStateIterators.get(level),
					edfaStateSequence.get(pos).getFstStates());
		}

		// iterate over output item/state pairs
		while(itemStateIt.hasNext()) {
			final ItemState itemState = itemStateIt.next();

			// we need to process that state because we saw it in the forward pass (assertion checks this)
			final State toState = itemState.state;
			assert edfaStateSequence.get(pos).getFstStates().get(toState.getId());

			final int outputItemFid = itemState.itemFid;
			if(outputItemFid == 0) { // EPS output
				// we did not get an output, so continue with the current prefix
				int newLevel = level + (itemStateIt.hasNext() ? 1 : 0); // no need to create new iterator if we are done on this level
				stepTwoPass(pos - 1, toState, newLevel);
			} else {
				// we got an output; check whether it is relevant
				if (!useFlist || largestFrequentFid >= outputItemFid) {
					// now append this item to the prefix, continue running the FST, and remove the item once done
					prefix.add(outputItemFid);
					int newLevel = level + (itemStateIt.hasNext() ? 1 : 0); // no need to create new iterator if we are done on this level
					stepTwoPass(pos - 1, toState, newLevel);
					prefix.removeInt(prefix.size() - 1);
				}
			}
		}
	}

	/** Counts the provided output sequence. Avoids double-counting.
	 *
	 * TODO: does not work correctly if supports grow beyond integer size
	 */
	private void countSequence(Sequence sequence) {
		long supSid = outputSequences.getLong(sequence);

		// add sequence if never mined before
		if (supSid == -1) { // set as return value when key not present
			outputSequences.put(new Sequence(sequence), PrimitiveUtils.combine((int)inputSupport, inputId)); // need to copy here
			return;
		}

		// otherwise increment frequency when if hasn't been mined from the current input sequence already
		if (PrimitiveUtils.getRight(supSid) != inputId) {
			int newCount = PrimitiveUtils.getLeft(supSid) + (int)inputSupport;
			outputSequences.put(sequence, PrimitiveUtils.combine(newCount, inputId));
		}
	}

}
