package de.uni_mannheim.desq.mining;

import com.google.common.base.Stopwatch;
import de.uni_mannheim.desq.examples.DesqDfsRunDistributedMiningLocally;
import de.uni_mannheim.desq.fst.*;
import de.uni_mannheim.desq.fst.BasicTransition.OutputLabelType;
import de.uni_mannheim.desq.fst.graphviz.FstVisualizer;
import de.uni_mannheim.desq.patex.PatEx;
import de.uni_mannheim.desq.util.CloneableIntHeapPriorityQueue;
import de.uni_mannheim.desq.util.DesqProperties;
import de.uni_mannheim.desq.util.PrimitiveUtils;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import it.unimi.dsi.fastutil.objects.*;
import org.apache.commons.io.FilenameUtils;
import scala.Tuple2;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.io.IOException;

public final class DesqDfs extends MemoryDesqMiner {
	private static final Logger logger = Logger.getLogger(DesqDfs.class);

	// -- parameters for mining ---------------------------------------------------------------------------------------

	/** Minimum support */
	final long sigma;

	/** The pattern expression used for mining */
	final String patternExpression;

	/** If true, input sequences that do not match the pattern expression are pruned */
	final boolean pruneIrrelevantInputs;

	/** If true, the two-pass algorithm for DesqDfs is used */
	final boolean useTwoPass;


	// -- helper variables --------------------------------------------------------------------------------------------

    /** Stores the final state transducer for DesqDfs (one-pass) */
	private final Fst fst;

    /** Stores the largest fid of an item with frequency at least sigma. Zsed to quickly determine
     * whether an item is frequent (if fid <= largestFrequentFid, the item is frequent */
	private final int largestFrequentFid;

    /** Stores iterators over output item/next state pairs for reuse. */
	private final ArrayList<Iterator<ItemState>> itemStateIterators = new ArrayList<>();

    /** An iterator over a projected database (a posting list) for reuse */
	private final PostingList.Iterator projectedDatabaseIt = new PostingList.Iterator();

	/** The root node of the search tree. */
	private final DesqDfsTreeNode root;

	/** Bundles arguments for {@link #incStepOnNFA(IncStepArgs, int)} */
    private final IncStepArgs current = new IncStepArgs();

	// -- helper variables for pruning and twopass --------------------------------------------------------------------

	/** The DFA corresponding to the FST (pruning) or reverse FST (two-pass). */
	private final Dfa dfa;

    /** For each relevant input sequence, the sequence of states taken by dfa (two-pass only) */
	private final ArrayList<DfaState[]> dfaStateSequences;

    /** A sequence of EDFA states for reuse (two-pass only) */
	private final ArrayList<DfaState> dfaStateSequence;

    /** A sequence of positions for reuse (two-pass only) */
	private final IntList dfaInitialPos;



	// -- helper variables for distributing --------------------------------------------------------------------------
	/** Set of output items created by one input sequence */
	final IntSet pivotItems = new IntAVLTreeSet();

	/** Storing the current input sequence */
	protected IntList inputSequence;

	/** pivot item of the currently mined partition */
	private int pivotItem = 0;

	/** Current prefix (used for finding the potential output items of one sequence) */
	final Sequence prefix = new Sequence();

	/** length of last processed prefix */
	int processedPrefixSize = 0;

	/** If true, DesqDfs Distributed sends Output NFAs instead of input sequences */
	final boolean sendNFAs;

	/** If true, DesqDfs Distributed merges shared suffixes in the tree representation */
	final boolean mergeSuffixes;

	/** Stores one Transition iterator per recursion level for reuse */
	final ArrayList<Iterator<Transition>> transitionIterators = new ArrayList<>();

	/** Ascendants of current input item */
	IntAVLTreeSet ascendants = new IntAVLTreeSet();

	/** Current transition */
	BasicTransition tr;

	/** Current to-state */
	State toState;

	/** Item added by current transition */
	int addItem;

	/** For each pivot item (and input sequence), we serialize a NFA producing the output sequences for that pivot item from the current input sequence */
	private Int2ObjectOpenHashMap<OutputNFA> nfas = new Int2ObjectOpenHashMap<>();
	private Int2ObjectOpenHashMap<Sequence> serializedNFAs = new Int2ObjectOpenHashMap<>();

	/** The output partitions */
	Int2ObjectOpenHashMap<ObjectList<IntList>> partitions;

	/** Stores one pivot element heap per level for reuse */
	final ArrayList<CloneableIntHeapPriorityQueue> pivotItemHeaps = new ArrayList<>();

	/** Stats about pivot element search */
	public long counterTotalRecursions = 0;
	private boolean verbose;
	private boolean drawGraphs = DesqDfsRunDistributedMiningLocally.drawGraphs;
	private int numSerializedStates = 0;

	/** Stop watches and counters */
	public static Stopwatch swFirstPass = Stopwatch.createUnstarted();
	public static Stopwatch swSecondPass = Stopwatch.createUnstarted();
	public static Stopwatch swPrep = Stopwatch.createUnstarted();
	public static Stopwatch swSetup = Stopwatch.createUnstarted();
	public static Stopwatch swTrim = Stopwatch.createUnstarted();
	public static Stopwatch swMerge = Stopwatch.createUnstarted();
	public static Stopwatch swSerialize = Stopwatch.createUnstarted();
	public static Stopwatch swReplace = Stopwatch.createUnstarted();
	public static long maxNumStates = 0;
	public static long maxRelevantSuccessors = 0;
	public static long counterTrimCalls = 0;
	public static long counterFollowGroupCalls = 0;
	public static long counterIsMergeableIntoCalls = 0;
	public static long counterFollowTransitionCalls = 0;
	public static long counterTransitionsCreated = 0;
	public static long counterSerializedStates = 0;
	public static long counterSerializedTransitions = 0;
	public static long counterPathsAdded = 0;
	public static long maxFollowGroupSetSize = 0;
	public static long maxPivotsForOneSequence = 0;
	public static long maxPivotsForOnePath = 0;
	public static long maxNumOutTrs = 0;



	// -- construction/clearing ---------------------------------------------------------------------------------------

	public DesqDfs(DesqMinerContext ctx) {
		super(ctx);
		sigma = ctx.conf.getLong("desq.mining.min.support");
		largestFrequentFid = ctx.dict.lastFidAbove(sigma);
		pruneIrrelevantInputs = ctx.conf.getBoolean("desq.mining.prune.irrelevant.inputs");
		useTwoPass = ctx.conf.getBoolean("desq.mining.use.two.pass");
		sendNFAs = ctx.conf.getBoolean("desq.mining.send.nfas", false);
		mergeSuffixes = ctx.conf.getBoolean("desq.mining.merge.suffixes", false);

		// create FST
		patternExpression = ctx.conf.getString("desq.mining.pattern.expression");
		PatEx p = new PatEx(patternExpression, ctx.dict);
		this.fst = p.translate();
		fst.minimize(); //TODO: move to translate
		fst.annotate();

		// create two pass auxiliary variables (if needed)
		if (useTwoPass) { // two-pass
			// two-pass will always prune irrelevant input sequences, so notify the user when the corresponding
			// property is not set
			if (!pruneIrrelevantInputs) {
				logger.warn("property desq.mining.prune.irrelevant.inputs=false will be ignored because " +
						"desq.mining.use.two.pass=true");
			}

            // initialize helper variables for two-pass
			dfaStateSequences = new ArrayList<>();
			dfaStateSequence = new ArrayList<>();
			dfaInitialPos = new IntArrayList();
		} else { // invalidate helper variables for two-pass
            dfaStateSequences = null;
			dfaStateSequence = null;
			dfaInitialPos = null;
		}

		// create DFA or reverse DFA (if needed)
		if(useTwoPass) {
			// construct the DFA for the FST (for the first pass)
			// the DFA is constructed for the reverse FST
			this.dfa = Dfa.createReverseDfa(fst, ctx.dict, largestFrequentFid, true);
		} else if (pruneIrrelevantInputs) {
			// construct the DFA to prune irrelevant inputs
			// the DFA is constructed for the forward FST
			this.dfa = Dfa.createDfa(fst, ctx.dict, largestFrequentFid, false);
		} else {
			this.dfa = null;
			
		}

		fst.numberTransitions();
		fst.exportGraphViz(DesqDfsRunDistributedMiningLocally.useCase + "-fst.pdf");

		// other auxiliary variables
		root = new DesqDfsTreeNode(fst.numStates());
		current.node = root;

		verbose = DesqDfsRunDistributedMiningLocally.verbose;
	}

	public static DesqProperties createConf(String patternExpression, long sigma) {
		DesqProperties conf = new DesqProperties();
		conf.setProperty("desq.mining.miner.class", DesqDfs.class.getCanonicalName());
		conf.setProperty("desq.mining.min.support", sigma);
		conf.setProperty("desq.mining.pattern.expression", patternExpression);
		conf.setProperty("desq.mining.prune.irrelevant.inputs", false);
		conf.setProperty("desq.mining.use.two.pass", false);
		return conf;
	}

	public void clear() {
		clear(false);
	}

	public void clear(boolean trimInputSequences) {
		inputSequences.clear();
		if(trimInputSequences) inputSequences.trimToSize();
		if (useTwoPass) {
			dfaStateSequences.clear();
            dfaStateSequences.trimToSize();
		}
		root.clear();
		current.node = root;
	}

	// -- processing input sequences ----------------------------------------------------------------------------------

	@Override
	public void addInputSequence(IntList inputSequence, long inputSupport, boolean allowBuffering) {
        // two-pass version of DesqDfs
        if (useTwoPass) {
            // run the input sequence through the DFA and compute the state sequences as well as the positions from
            // which a final FST state is reached
            if (dfa.acceptsReverse(inputSequence, dfaStateSequence, dfaInitialPos)) {
                // we now know that the sequence is relevant; remember it
                super.addInputSequence(inputSequence, inputSupport, allowBuffering);
                dfaStateSequences.add(dfaStateSequence.toArray(new DfaState[dfaStateSequence.size()]));

                // run the first incStep; start at all positions from which a final FST state can be reached
//				assert current.node == root;
                current.inputId = inputSequences.size() - 1;
                current.inputSequence = inputSequences.get(current.inputId);
                current.dfaStateSequence = dfaStateSequences.get(current.inputId);
                for (int i = 0; i < dfaInitialPos.size(); i++) {
                    // for those positions, start with the initial state
                    incStep(dfaInitialPos.getInt(i), fst.getInitialState(), 0);
                }

                // clean up
                dfaInitialPos.clear();
            }
            dfaStateSequence.clear();
            return;
        }

        // one-pass version of DesqDfs
        if (!pruneIrrelevantInputs || dfa.accepts(inputSequence)) {
            // if we reach this place, we either don't want to prune irrelevant inputs or the input is relevant
            // -> remember it
            super.addInputSequence(inputSequence, inputSupport, allowBuffering);

            // and run the first inc step
            current.inputId = inputSequences.size() - 1;
            current.inputSequence = inputSequences.get(current.inputId);
            incStep(0, fst.getInitialState(), 0);
        }
	}




	// -- mining ------------------------------------------------------------------------------------------------------



	@Override
	public void mine() {
		if (sumInputSupports >= sigma) {
			// the root has already been processed; now recursively grow the patterns
			root.pruneInfrequentChildren(sigma);
			expand(new IntArrayList(), root);
		}
	}


	/** Updates the projected databases of the children of the current node (<code>current.node</code>) corresponding to each possible
	 * next output item for the current input sequence (also stored in <code>currect</code>).
	 *
	 * @param pos next item to read
	 * @param state current FST state
	 * @param level recursion level (used for reusing iterators without conflict)
	 *
	 * @return true if the initial FST state can be reached without producing further output
	 */
	private boolean incStep(final int pos, final State state, final int level) {
		// check if we reached a final complete state or consumed entire input and reached a final state
		if ( state.isFinalComplete() || pos == current.inputSequence.size() )
			return state.isFinal();

		// get iterator over next output item/state pairs; reuse existing ones if possible
		// in two-pass, only iterates over states that we saw in the first pass (the other ones can safely be skipped)
		final int itemFid = current.inputSequence.getInt(pos);
		final BitSet validToStates = useTwoPass
				? current.dfaStateSequence[ current.inputSequence.size()-(pos+1) ].getFstStates() // only states from first pass
				: null; // all states
		Iterator<ItemState> itemStateIt;
		if (level>=itemStateIterators.size()) {
			itemStateIt = state.consume(itemFid, null, validToStates);
			itemStateIterators.add(itemStateIt);
		} else {
			itemStateIt = state.consume(itemFid, itemStateIterators.get(level), validToStates);
		}

		// iterate over output item/state pairs and remember whether we hit the final or finalComplete state without producing output
		// (i.e., no transitions or only transitions with epsilon output)
		boolean reachedInitialStateWithoutOutput = false;
		while (itemStateIt.hasNext()) {
			final ItemState itemState = itemStateIt.next();
			final int outputItemFid = itemState.itemFid;
			final State toState = itemState.state;

			if (outputItemFid == 0) { // EPS output
				// we did not get an output, so continue running the FST
				int newLevel = level + (itemStateIt.hasNext() ? 1 : 0); // no need to create new iterator if we are done on this level
				reachedInitialStateWithoutOutput |= incStep(pos + 1, toState, newLevel);
			} else if (largestFrequentFid >= outputItemFid && (pivotItem == 0 || pivotItem >= outputItemFid)) {
				// we have an output and its frequent, so update the corresponding projected database
				current.node.expandWithItem(outputItemFid, current.inputId, current.inputSequence.weight,
						pos+1, toState.getId());

			}
		}

		return reachedInitialStateWithoutOutput;
	}

	/** Expands all children of the given search tree node. The node itself must have been processed/output/expanded
	 * already.
	 *
	 * @param prefix (partial) output sequence corresponding to the given node (must remain unmodified upon return)
	 * @param node the node whose children to expandOnNFA
	 */

	private void expand(IntList prefix, DesqDfsTreeNode node) {
		// add a placeholder to prefix for the output item of the child being expanded
		final int lastPrefixIndex = prefix.size();
		prefix.add(-1);

		// iterate over all children
		for (final DesqDfsTreeNode childNode : node.childrenByFid.values() )  {
			// while we expandOnNFA the child node, we also compute its actual support to determine whether or not
			// to output it (and then output it if the support is large enough)
			long support = 0;
			// check whether pivot expansion worked
			// the idea is that we never expandOnNFA a child>pivotItem at this point
			if(pivotItem != 0) {
				assert childNode.itemFid <= pivotItem;
			}

			// set up the expansion
			assert childNode.prefixSupport >= sigma;
			prefix.set(lastPrefixIndex, childNode.itemFid);
			projectedDatabaseIt.reset(childNode.projectedDatabase);
			current.inputId = -1;
			current.node = childNode;

			do {
				// process next input sequence
				current.inputId += projectedDatabaseIt.nextNonNegativeInt();
				current.inputSequence = inputSequences.get(current.inputId);
				if (useTwoPass) {
					current.dfaStateSequence = dfaStateSequences.get(current.inputId);
				}

				// iterate over state@pos snapshots for this input sequence
                boolean reachedFinalStateWithoutOutput = false;
				do {
					final int stateId = projectedDatabaseIt.nextNonNegativeInt();
					final int pos = projectedDatabaseIt.nextNonNegativeInt(); // position of next input item
					reachedFinalStateWithoutOutput |= incStep(pos, fst.getState(stateId), 0);
				} while (projectedDatabaseIt.hasNext());

                // if we reached a final state without output, increment the support of this child node
				if (reachedFinalStateWithoutOutput) {
					support += current.inputSequence.weight;
				}

				// now go to next posting (next input sequence)
			} while (projectedDatabaseIt.nextPosting());

			// output the patterns for the current child node if it turns out to be frequent
			if (support >= sigma) {
				// if we are mining a specific pivot partition (meaning, pivotItem>0), then we output only sequences with that pivot
				if (ctx.patternWriter != null && (pivotItem==0 || pivot(prefix) == pivotItem)) {
					if (!useTwoPass) { // one-pass
						ctx.patternWriter.write(prefix, support);
					} else { // two-pass
						// for the two-pass algorithm, we need to reverse the output because the search tree is built
						// in reverse order
						ctx.patternWriter.write(prefix, support);
					}
				}
			}

			// expandOnNFA the child node
			childNode.pruneInfrequentChildren(sigma);
			childNode.projectedDatabase = null; // not needed anymore
			expand(prefix, childNode);
			childNode.invalidate(); // not needed anymore
		}

		// we are done processing the node, so remove its item from the prefix
		prefix.removeInt(lastPrefixIndex);
	}




	// ---------------- DesqDfs Distributed ---------------------------------------------------------


	public void addNFA(IntList inputSequence, long inputSupport, boolean allowBuffering) {
		super.addInputSequence(inputSequence, inputSupport, allowBuffering);
	}

	/**
	 * Determine pivot items for one sequence, without storing them
	 * @param inputSequences
	 * @param verbose
	 * @return
	 * @throws IOException
	 */
	public Tuple2<Integer,Integer> determinePivotItemsForSequences(ObjectArrayList<Sequence> inputSequences, boolean verbose) throws IOException {
		int numSequences = 0;
		int totalPivotElements = 0;
		//Sequence inputSequence = new Sequence();
		IntSet pivotElements;
		for(Sequence inputSequence: inputSequences) {
			pivotElements = getPivotItemsOfOneSequence(inputSequence);
			totalPivotElements += pivotElements.size();
			if(verbose) {
				System.out.println(inputSequence.toString() + " pivots:" + pivotElements.toString());
			}
			numSequences++;
		}
		return new Tuple2(numSequences,totalPivotElements);
	}

	/**
	 * Produces partitions for a given set of input sequences, producing one of two shuffle options:
	 * 1) input sequences
	 * 2) output sequences as nfas, with suffixes optionally merged
	 *
	 * @param inputSequences the input sequences
	 * @return partitions Map
	 */
	public Int2ObjectOpenHashMap<ObjectList<IntList>> createPartitions(ObjectArrayList<Sequence> inputSequences, boolean verbose) throws IOException {

		partitions = new Int2ObjectOpenHashMap<>();
		IntSet pivotElements;
		Sequence inputSequence;
		ObjectList<IntList> newPartitionSeqList;

		// for each input sequence, emit (pivot_item, transition_id)
		for(int seqNo=0; seqNo<inputSequences.size(); seqNo++) {
			inputSequence = inputSequences.get(seqNo);
			if (sendNFAs) {
				// NFAs
				createNFAPartitions(inputSequence, true, seqNo);
			} else {
				// input sequences
				pivotElements = getPivotItemsOfOneSequence(inputSequence);

				if(verbose) {
					System.out.println(inputSequence.toString() + " pivots:" + pivotElements.toString());
				}
				for (int pivot : pivotElements) {
					if (partitions.containsKey(pivot)) {
						partitions.get(pivot).add(inputSequence);
					} else {
						newPartitionSeqList = new ObjectArrayList<IntList>();
						newPartitionSeqList.add(inputSequence);
						partitions.put(pivot, newPartitionSeqList);
					}
					if(DesqDfsRunDistributedMiningLocally.writeShuffleStats) DesqDfsRunDistributedMiningLocally.writeShuffleStats(seqNo, pivot, inputSequence.size());
				}
			}
		}
		return partitions;
	}


	/**
	 * Produces set of frequent output elements created by one input sequence by running the FST
	 * and storing all frequent output items
	 *
	 * We are using one pass for this for now, as it is generally safer to use.
	 *
	 * @param inputSequence
	 * @return pivotItems set of frequent output items of input sequence inputSequence
	 */
	public IntSet getPivotItemsOfOneSequence(IntList inputSequence) {
		pivotItems.clear();
		this.inputSequence = inputSequence;
		// check whether sequence produces output at all. if yes, produce output items
		if(useTwoPass) {
			// Run the DFA to find to determine whether the input has outputs and in which states
			// Then walk backwards and collect the output items
			if (dfa.acceptsReverse(inputSequence, dfaStateSequence, dfaInitialPos)) {
				//dfaStateSequences.add(dfaStateSequence.toArray(new DfaState[dfaStateSequence.size()]));

				// run the first incStep; start at all positions from which a final FST state can be reached
				for (int i = 0; i<dfaInitialPos.size(); i++) {
					// for those positions, start with the initial state
					piStep(null, dfaInitialPos.getInt(i), fst.getInitialState(), 0);
				}

				// clean up
				dfaInitialPos.clear();
			}
			dfaStateSequence.clear();

		} else { // one pass

			if (!pruneIrrelevantInputs || dfa.accepts(inputSequence)) {
				piStep(null, 0, fst.getInitialState(), 0);
			}
		}
		return pivotItems;
	}


	/** Produces the set of pivot items for a given input sequence and constructs a tree representation of the transitions
	 * generating all output sequences of this input sequence
	 *
	 * @param inputSequence
	 * @return pivotItems set of frequent output items of input sequence inputSequence
	 */
	public Int2ObjectOpenHashMap<Sequence> createNFAPartitions(IntList inputSequence, boolean buildPartitions, int seqNo) {

		// get the pivot elements with the corresponding paths through the FST
		this.inputSequence = inputSequence;
		prefix.clear();
		nfas.clear();
		pivotItems.clear();

		if(!buildPartitions)
			serializedNFAs.clear();

		if(useTwoPass) {
			dfaStateSequence.clear();
			swFirstPass.start();
			if (dfa.acceptsReverse(inputSequence, dfaStateSequence, dfaInitialPos)) {

				swFirstPass.stop();
				// run the first incStep; start at all positions from which a final FST state can be reached
				swSecondPass.start();
				for (int i = 0; i < dfaInitialPos.size(); i++) {
					piStep(null, dfaInitialPos.getInt(i), fst.getInitialState(), 0);
				}
				swSecondPass.stop();

				// clean up
				dfaInitialPos.clear();
			} else {
				swFirstPass.stop();
			}
		} else {
			piStep(null, 0, fst.getInitialState(), 0);
		}


		if(nfas.size() > maxPivotsForOneSequence) maxPivotsForOneSequence = nfas.size();

		// For each pivot item, trim the NFA and output it
		for(Int2ObjectMap.Entry<OutputNFA> pivotAndNFA : nfas.int2ObjectEntrySet()) {
			int pivotItem =  pivotAndNFA.getIntKey();
			OutputNFA nfa = pivotAndNFA.getValue();

			// Trim the NFA for this pivot and directly serialize it
			Sequence output = nfa.serialize();

			if(drawGraphs) drawSerializedNFA(output, DesqDfsRunDistributedMiningLocally.useCase + "-seq"+seqNo+"-pivot"+pivotItem+"-NFA.pdf", true, "");

			if(DesqDfsRunDistributedMiningLocally.writeShuffleStats) DesqDfsRunDistributedMiningLocally.writeShuffleStats(seqNo, pivotItem, numSerializedStates);

			if(buildPartitions) {
				// emit the list we constructed
				if (!partitions.containsKey(pivotItem)) {
					partitions.put(pivotItem, new ObjectArrayList<>());
				}
				partitions.get(pivotItem).add(output);
			} else {
				serializedNFAs.put(pivotItem, output);
			}
		}
		return serializedNFAs;
	}


	/** Runs one step (along compressed transition) in the FST in order to produce the set of frequent output items
	 *  and directly creates the partitions with the transition representation
	 *
	 * @param currentPivotItems set of current potential pivot items
	 * @param pos   current position
	 * @param state current state
	 * @param level current level
	 */
	private void piStep(CloneableIntHeapPriorityQueue currentPivotItems, int pos, State state, int level) {
		counterTotalRecursions++;

		// if we reached a final state, we add the current set of pivot items at this state to the global set of pivot items for this sequences
		if(state.isFinal() && prefix.size() > processedPrefixSize) {
			processedPrefixSize = prefix.size();

			if(sendNFAs) {

				IntSet emittedPivots = new IntOpenHashSet();
				Sequence path = prefix.clone();

				// we might arrive multiple times here (if we have eps-transitions) But we don't need to do all this work again.
				// (if we arrive here multiple times, the path (and ergo, the pivot items) are the same every time.
				counterPathsAdded++;

				// we can have a pivot item multiple times here, but we don't take care of these for now
				for (int i = 0; i < currentPivotItems.size(); i++) {
					int pivotItem = currentPivotItems.exposeInts()[i]; // This isn't very nice, but it does not drop read elements from the heap. TODO: find a better way?

					if(!emittedPivots.contains(pivotItem)) {
						emittedPivots.add(pivotItem);
						OutputNFA nfa;
						if(!nfas.containsKey(pivotItem)) {
							nfa = new OutputNFA(pivotItem);
							nfas.put(pivotItem, nfa);
						} else {
							nfa = nfas.get(pivotItem);
						}

						nfa.addPath(path);
					}

				}
				if(emittedPivots.size() > maxPivotsForOnePath) maxPivotsForOnePath = emittedPivots.size();
			} else {
				// if we don't build the transition representation, just keep track of the pivot items
				for(int i=0; i<currentPivotItems.size(); i++) {
					pivotItems.add(currentPivotItems.exposeInts()[i]); // This isn't very nice, but it does not drop read elements from the heap. TODO: find a better way?
				}
			}
		}

		// check if we already read the entire input
		if (state.isFinalComplete() || pos == inputSequence.size()) {
			return;
		}

		// get the next input item
		final int itemFid = inputSequence.getInt(pos);

		// in two pass, we only go to states we saw in the first pass
		final BitSet validToStates = useTwoPass
				? dfaStateSequence.get(inputSequence.size()-(pos+1)).getFstStates() // only states from first pass
				: null; // all states

		// get an iterator over all relevant transitions from here (relevant=starts from this state + matches the input item)
		Iterator<Transition> transitionIt;
		if(level >= transitionIterators.size()) {
			transitionIt = state.consumeCompressed(itemFid, null, validToStates);
			transitionIterators.add(transitionIt);
		} else {
			transitionIt = state.consumeCompressed(itemFid, transitionIterators.get(level), validToStates);
		}

		// get set for storing potential pivot elements
		CloneableIntHeapPriorityQueue newCurrentPivotItems;
		if(level >= pivotItemHeaps.size()) {
			newCurrentPivotItems = new CloneableIntHeapPriorityQueue();
			pivotItemHeaps.add(newCurrentPivotItems);
		} else {
			newCurrentPivotItems = pivotItemHeaps.get(level);
		}

		addItem = -1;

		// follow each relevant transition
		while(transitionIt.hasNext()) {
			tr = (BasicTransition) transitionIt.next();
			toState = tr.getToState();
			OutputLabelType ol = tr.getOutputLabelType();

			// We handle the different output label types differently
			if (ol == OutputLabelType.EPSILON) { // EPS
				// an eps transition does not introduce potential pivot elements, so we simply continue recursion
				piStep(currentPivotItems, pos + 1, toState, level + 1);

			} else if (ol == OutputLabelType.CONSTANT || ol == OutputLabelType.SELF) { // CONSTANT and SELF
				// SELF and CONSTANT transitions both yield exactly one new potential pivot item.

				// retrieve input item
				if (ol == OutputLabelType.CONSTANT) { // CONSTANT
					addItem = tr.getOutputLabel();
				} else { // SELF
					addItem = itemFid;
				}
				// If the output item is frequent, we merge it into the set of current potential pivot items and recurse
				if (largestFrequentFid >= addItem) {
					//CloneableIntHeapPriorityQueue newCurrentPivotItems;
					if (currentPivotItems == null) { // set of pivot elements is empty so far, so no update is necessary, we just create a new set
						//newCurrentPivotItems = new CloneableIntHeapPriorityQueue();
						newCurrentPivotItems.clear();
						newCurrentPivotItems.enqueue(addItem);
					} else { // update the set of current pivot elements
						// get the first half: current[>=min(add)]
						newCurrentPivotItems.startFromExisting(currentPivotItems);
						while (newCurrentPivotItems.size() > 0 && newCurrentPivotItems.firstInt() < addItem) {
							newCurrentPivotItems.dequeueInt();
						}
						// join in the second half: add[>=min(current)]		  (don't add the item a second time if it is already in the heap)
						if (addItem >= currentPivotItems.firstInt() && (newCurrentPivotItems.size() == 0 || addItem != newCurrentPivotItems.firstInt())) {
							newCurrentPivotItems.enqueue(addItem);
						}
					}
					// we put the current transition together with the input item onto the prefix and take it off when we come back from recursion

					// TODO: instead of using TR/INP, we should write transitions according to output item equivalency (which might differ sometimes
					if (sendNFAs) {
						prefix.add(-addItem);
					}

					piStep(newCurrentPivotItems, pos + 1, toState, level + 1); //, currentPathState.followTransition(tr, addItem, nfa));

					if(sendNFAs) {
						prefix.removeInt(prefix.size() -1);
						processedPrefixSize = prefix.size();
					}

				}
			} else { // SELF_GENERALIZE
				addItem = itemFid; // retrieve the input item
				// retrieve ascendants of the input item
				ascendants.clear();
				ascendants.add(addItem);
				tr.addAscendantFids(addItem, ascendants);
				// we only consider this transition if the set of output elements contains at least one frequent item
				if (largestFrequentFid >= ascendants.firstInt()) { // the first item of the ascendants is the most frequent one

					//CloneableIntHeapPriorityQueue newCurrentPivotItems;
					if (currentPivotItems == null) { // if we are starting a new pivot set there is no need for a union
						// we are reusing the heap object, so reset the heap size to 0
						newCurrentPivotItems.clear();
						// headSet(largestFrequentFid + 1) drops all infrequent items
						for (int ascendant : ascendants.headSet(largestFrequentFid + 1)) {
							newCurrentPivotItems.enqueue(ascendant);
						}
					} else {
						// first half of the update union: current[>=min(add)].
						newCurrentPivotItems.startFromExisting(currentPivotItems);
						while (newCurrentPivotItems.size() > 0 && newCurrentPivotItems.firstInt() < ascendants.firstInt()) {
							newCurrentPivotItems.dequeueInt();
						}
						// second half of the update union: add[>=min(curent)]
						// we filter out infrequent items, so in fact we do:  add[<=largestFrequentFid][>=min(current)]
						for (int add : ascendants.headSet(largestFrequentFid + 1).tailSet(currentPivotItems.firstInt())) {
							newCurrentPivotItems.enqueue(add);
						}
					}
					if (sendNFAs) {
						prefix.add(addItem);
						prefix.add(tr.getTransitionNumber());
					}

					piStep(newCurrentPivotItems, pos + 1, toState, level + 1);

					if(sendNFAs) {
						prefix.removeInt(prefix.size() - 1);
						prefix.removeInt(prefix.size() - 1);
						processedPrefixSize = prefix.size();
					}
				}
			}
		}
	}


	/**
	 * Mine with respect to a specific pivot item. e.g, filter out all non-pivot sequences
	 *   (e.g. filter out all sequences where pivotItem is not the max item)
	 * @param pivotItem
	 */
	public void minePivot(int pivotItem) {
		this.pivotItem = pivotItem;


		// run the normal mine method. Filtering is done in expandOnNFA() when the patterns are output
		if(sendNFAs)
			mineOnNFA();
		else
			mine();

		// reset the pivot item, just to be sure. pivotItem=0 means no filter
		this.pivotItem = 0;
	}

	public void mineOnNFA() {
		if (sumInputSupports >= sigma) {
			DesqDfsTreeNode root = new DesqDfsTreeNode(sendNFAs ? 1 : fst.numStates()); // if we use NFAs, we need only one BitSet per node
			final IncStepArgs incStepArgs = new IncStepArgs();
			incStepArgs.node = root;
			// and process all input sequences to compute the roots children
			for (int inputId = 0; inputId < inputSequences.size(); inputId++) {
				incStepArgs.inputId = inputId;
				incStepArgs.inputSequence = inputSequences.get(inputId);
				incStepOnNFA(incStepArgs, 0);
			}
			// the root has already been processed; now recursively grow the patterns
			root.pruneInfrequentChildren(sigma);
			expandOnNFA(new IntArrayList(), root);
		}
	}

	/** Expands all children of the given search tree node. The node itself must have been processed/output/expanded
	 * already.
	 *
	 * @param prefix (partial) output sequence corresponding to the given node (must remain unmodified upon return)
	 * @param node the node whose children to expandOnNFA
	 */
	private void expandOnNFA(IntList prefix, DesqDfsTreeNode node) {
		// this bundles common arguments to incStepOnePass or incStepTwoPass
		final IncStepArgs incStepArgs = new IncStepArgs();
		// add a placeholder to prefix for the output item of the child being expanded
		final int lastPrefixIndex = prefix.size();
		prefix.add(-1);

		// iterate over all children
		for (final DesqDfsTreeNode childNode : node.childrenByFid.values() )  {
			// while we expandOnNFA the child node, we also compute its actual support to determine whether or not
			// to output it (and then output it if the support is large enough)
			long support = 0;
			// check whether pivot expansion worked
			// the idea is that we never expandOnNFA a child>pivotItem at this point
			if(pivotItem != 0) {
				assert childNode.itemFid <= pivotItem;
			}
			// set up the expansion
			assert childNode.prefixSupport >= sigma;
			prefix.set(lastPrefixIndex, childNode.itemFid);
			projectedDatabaseIt.reset(childNode.projectedDatabase);
			incStepArgs.inputId = -1;
			incStepArgs.node = childNode;

			do {
				// process next input sequence
				incStepArgs.inputId += projectedDatabaseIt.nextNonNegativeInt();
				incStepArgs.inputSequence = inputSequences.get(incStepArgs.inputId);

//				if (useTwoPass) {
//					current.dfaStateSequence = dfaStateSequences.get(current.inputId);
//				}

				// iterate over state@pos snapshots for this input sequence
				boolean reachedFinalStateWithoutOutput = false;
				do {
					final int pos = projectedDatabaseIt.nextNonNegativeInt(); // position of next input item
					reachedFinalStateWithoutOutput |= incStepOnNFA(incStepArgs, pos);
				} while (projectedDatabaseIt.hasNext());

				// if we reached a final state without output, increment the support of this child node
				if (reachedFinalStateWithoutOutput) {
					support += incStepArgs.inputSequence.weight;
				}

				// now go to next posting (next input sequence)
			} while (projectedDatabaseIt.nextPosting());

			// output the patterns for the current child node if it turns out to be frequent
			if (support >= sigma) {
				// if we are mining a specific pivot partition (meaning, pivotItem>0), then we output only sequences with that pivot
				if (ctx.patternWriter != null && (pivotItem==0 || pivot(prefix) == pivotItem)) { // TODO: investigate what is pruned here (whether we can improve)
					ctx.patternWriter.write(prefix, support);
				}
			}
			// expandOnNFA the child node
			childNode.pruneInfrequentChildren(sigma);
			childNode.projectedDatabase = null; // not needed anymore
			expandOnNFA(prefix, childNode);
			childNode.invalidate(); // not needed anymore
		}
		// we are done processing the node, so remove its item from the prefix
		prefix.removeInt(lastPrefixIndex);
	}


	/** Updates the projected databases of the children of the current node (args.node) corresponding to each possible
	 * next output item for the current input sequence (also stored in args). Used only in the one-pass algorithm.
	 *
	 * @param args information about the input sequence and the current search tree node
	 * @param pos next item to read
	 *
	 * @return true if a final FST state can be reached without producing further output
	 */
	private boolean incStepOnNFA(final IncStepArgs args, final int pos) {
		// in transition representation, we store pairs of integers: (transition id, input element)
		int trNo;
		int inputItem;
		int followPos;
		IntList outputItems;
		int readPos = pos;
		int nextInt = args.inputSequence.getInt(readPos);
		while(nextInt != Integer.MIN_VALUE && nextInt != Integer.MAX_VALUE) {

			if(nextInt < 0) { // CONSTANT or SELF transition, nextInt is -inputItem
				inputItem = -nextInt;
				assert inputItem <= pivotItem; // otherwise, this path should not have been sent to the partition
				followPos = args.inputSequence.getInt(readPos+1); // next int is the offset of the next state
				args.node.expandWithTransition(inputItem, args.inputId, args.inputSequence.weight, followPos); // expandOnNFA with this output item
				readPos = readPos+2; // we read 2 items from the list
			} else { // SELF_ASCENDANTS transition, so we stored the inputItem (in nextInt) and the number of the transition
				inputItem = nextInt;
				trNo = args.inputSequence.getInt(readPos+1);
				followPos = args.inputSequence.getInt(readPos+2);
				outputItems = fst.getBasicTransitionByNumber(trNo).getOutputElements(inputItem);

				for (int outputItem : outputItems) {
					if (pivotItem >= outputItem) { // no check for largestFrequentFid necessary, as largestFrequentFid >= pivotItem
						args.node.expandWithTransition(outputItem, args.inputId, args.inputSequence.weight, followPos);
					}
				}
				readPos = readPos + 3; // we read 3 elements
			}

			nextInt = args.inputSequence.getInt(readPos);
		}

		return nextInt == Integer.MIN_VALUE; // is true if this state is final
	}

	/**
	 * Determines the pivot item of a sequence
	 * @param sequence
	 * @return
	 */
	private int pivot(IntList sequence) {
		int pivotItem = 0;
		for(int item : sequence) {
			pivotItem = Math.max(item, pivotItem);
		}
		return pivotItem;
	}


	/** Bundles arguments for {@link #incStep}. These arguments are not modified during the method's recursions, so
	 * we keep them at a single place. */
	private static class IncStepArgs {
		int inputId;
		WeightedSequence inputSequence;
		DfaState[] dfaStateSequence;
		DesqDfsTreeNode node;
	}

	public void drawSerializedNFA(IntList seq, String file, boolean drawOutputItems, String caption) {
		FstVisualizer fstVisualizer = new FstVisualizer(FilenameUtils.getExtension(file), FilenameUtils.getBaseName(file));
		fstVisualizer.beginGraph();

		boolean nextPosIsStateBeginning = true;
		int currentStatePos = -1;
		int first, inp, trId, to;
		String label;
		for(int pos=0; pos<seq.size();) {
			if(nextPosIsStateBeginning) {
				currentStatePos = pos;
				nextPosIsStateBeginning = false;
			}
			first = seq.getInt(pos);
			if(first == Integer.MAX_VALUE) {
				nextPosIsStateBeginning = true;
				pos++;
			} else if(first == Integer.MIN_VALUE) {
				nextPosIsStateBeginning = true;
				pos++;
				fstVisualizer.addFinalState(String.valueOf(currentStatePos));
			} else {
				// transition

				if(first < 0) { // SELF or CONSTANT
					inp = -first;
					label = "i"+inp;
					to = seq.getInt(pos+1);
					pos = pos+2;
				} else {
					inp = first;
					trId = seq.getInt(pos+1);
					to = seq.getInt(pos+2);
					label = "i"+inp+"@T"+trId+"^";
					if(drawOutputItems) label += " " + fst.getBasicTransitionByNumber(trId).getOutputElements(inp);
					pos = pos+3;
				}
				fstVisualizer.add(String.valueOf(currentStatePos),label,String.valueOf(to));

			}
		}
		fstVisualizer.addCaption(caption);
		fstVisualizer.endGraph();
	}


	/**
	 * A NFA that produces the output sequences of one input sequence.
	 *
	 * Per input sequence, we send one NFA to each partition, if the input sequence produces at least one
	 * output sequence at that partition. Before sending the NFA, we trim it so we don't send paths that are
	 * not relevant for that partition. We do the trimming while serializing.
	 */
	private class OutputNFA {
		PathState root;
		int numPathStates = 0;
		int numPaths = 0;

		int pivot;

		/** List of states in this NFA */
		private ObjectList<PathState> pathStates = new ObjectArrayList<>();

		/** List of leaf states in this NFA */
		private IntAVLTreeSet leafs = new IntAVLTreeSet();

		public OutputNFA(int pivot) {
			root = new PathState(this, null, 0);
			this.pivot = pivot;
		}

		/** Get the nth PathState of this NFA */
		public PathState getPathStateByNumber(int n) {
			return pathStates.get(n);
		}

		public PathState getMergeTarget(int n) {
			PathState state = getPathStateByNumber(n);
			if(state.id != state.mergedInto)
				return getPathStateByNumber(state.mergedInto);
			else
				return state;
		}

		/** A BitSet with a bit set for every position which holds a state id  */
		BitSet positionsWithStateIds = new BitSet();

		/**
		 * Add a path to this NFA
		 *
		 * @param path
		 */
		protected void addPath(IntList path) {
			numPaths++;
			int trId;
			int inpItem;
			long outKey;
			PathState currentState;
			BasicTransition tr;
			// Run through the transitions of this path and add them to this NFA
			currentState = root;
			for(int i=0; i<path.size(); ) {
				inpItem = path.getInt(i);

				if(inpItem < 0) {
					// SELF or CONSTANT
                    outKey = PrimitiveUtils.combine(inpItem, 0);
					i = i+1;
				} else {
					// SELF_ASCENDING
					trId = path.getInt(i+1);
					// generalize the input item for this pivot
                    tr = fst.getBasicTransitionByNumber(trId);
					inpItem = tr.generalizeItemForPivot(inpItem, this.pivot);
					outKey = PrimitiveUtils.combine(inpItem, trId);
					i = i+2;
				}

				currentState = currentState.followTransition(outKey, this);

			}
			// we ran through the path, so the current state is a final state.
			currentState.setFinal();
		}

		public Sequence serialize() {
			counterTrimCalls++;

//			swSetup.start();
//			stateMapping = new int[numPathStates];
//			Arrays.fill(stateMapping,-1);
//			swSetup.stop();


			swMerge.start();

			followGroup(leafs, true);

			swMerge.stop();

			swSerialize.start();
			// serialize the trimmed NFA
			Sequence send = new Sequence();
			positionsWithStateIds.clear();
			numSerializedStates = 0;
			for(PathState state : pathStates) {
//				if(stateMapping[state.id] == -1) { // we serialize only non-merged states
                if(state.mergedInto == state.id) {
					state.serializeForward(send);
					numSerializedStates++;
				}
			}
			swSerialize.stop();


			swReplace.start();
			// replace all state ids with their positions
			PathState state;
			for (int pos = positionsWithStateIds.nextSetBit(0); pos >= 0; pos = positionsWithStateIds.nextSetBit(pos+1)) {
				state = getPathStateByNumber(send.getInt(pos));
				send.set(pos,state.writtenAtPos);

			}
			swReplace.stop();
			return send;
		}

		/**
		 * Process a group of states. That means, we try to merge subsets of the passed group of states. Each of those groups,
		 * we process recursively.
		 * @param stateIds
		 */
		private void followGroup(IntAVLTreeSet stateIds, boolean definitelyMergeable) {
		    if(stateIds.size() > maxFollowGroupSetSize) maxFollowGroupSetSize = stateIds.size();
			counterFollowGroupCalls++;
			IntBidirectionalIterator it = stateIds.iterator();
			int currentTargetStateId;
			int sId;
			PathState target, merge;

			// we use this bitset to mark states that we have already merged in this iteration.
			// we use that to make sure we don't process them more than once
			BitSet alreadyMerged = new BitSet();

			// as we go over the passed set of states, we collect predecessor groups, which we will process afterwards
			IntAVLTreeSet predecessors = new IntAVLTreeSet();

			int i = 0, j;
			while(it.hasNext()) {
				// Retrieve the next state of the group. We will use this one as a merge target for all the following states
				// in the list
				currentTargetStateId = it.nextInt();
				i++;
				if(alreadyMerged.get(i)) // we don't process this state if we have merged it into another state already
					continue;
				target = getMergeTarget(currentTargetStateId);

				// begin a new set of predecessors
                predecessors.clear();
				j = i;

				// Now compare this state to all the following states. If we find merge-compatible states, we merge them into this one
				while(it.hasNext()) {
					sId = it.nextInt();
					j++;

					if(alreadyMerged.get(j))  // don't process if we already merged this state into another one
						continue;

					merge = getMergeTarget(sId); // we retrieve the mapped number here. In case the state has already been merged somewhere else, we don't want to modify a merged state.

					if(merge.id == target.id) { // if the two states we are looking at are the same ones, we don't need to make any changes.
						alreadyMerged.set(j);
					} else if(definitelyMergeable || merge.isMergeableInto(target)) { // check whether the two states are mergeable

                        // we write down that we merged this state into the target.
                        merge.mergedInto = target.id;

                        // also rewrite the original entry if the state we just merged was merged before
                        if(merge.id != sId)
                            getPathStateByNumber(sId).mergedInto = target.id;

						// mark this state as merged, so we don't process it multiple times
						alreadyMerged.set(j);

						// if we have a group of predecessors (more than one), we process them as group
						if(target.predecessor != null && merge.predecessor != null) {
							if (target.predecessor.id != merge.predecessor.id) {
								predecessors.add(merge.predecessor.id);
							}
						}
					}
				}

				// rewind the iterator
				it.back(j-i);

				// if we have multiple predecessors, process them as group (later on)
				if(predecessors.size()>0) {
					predecessors.add(target.predecessor.id);
					followGroup(predecessors, false);
				}
			}
		}

		/**
		 * Export the PathStates of this NFA with their transitions to PDF
		 * @param file
		 * @param drawOutputItems
		 */
		public void exportGraphViz(String file, boolean drawOutputItems) {
			FstVisualizer fstVisualizer = new FstVisualizer(FilenameUtils.getExtension(file), FilenameUtils.getBaseName(file));
			fstVisualizer.beginGraph();
			for(PathState s : pathStates) {
				for(Long2ObjectMap.Entry<PathState> trEntry : s.outTransitions.long2ObjectEntrySet()) {
					int inputId = PrimitiveUtils.getLeft(trEntry.getLongKey());
					int trId = PrimitiveUtils.getRight(trEntry.getLongKey());

					String label;
					if(inputId < 0) {
						label = "i" + (-inputId);
						if(drawOutputItems)
							label += "[" + (-inputId) + "]";
					}
					else {
						label = "i" + inputId + "@T" + trId + (fst.getBasicTransitionByNumber(trId).getOutputLabelType() == OutputLabelType.SELF_ASCENDANTS ? "^" : "");
						if (drawOutputItems)
							label += " " + fst.getBasicTransitionByNumber(trId).getOutputElements(inputId);
					}
					fstVisualizer.add(String.valueOf(s.id), label, String.valueOf(trEntry.getValue().id));
				}
				if(s.isFinal)
					fstVisualizer.addFinalState(String.valueOf(s.id));
			}
			fstVisualizer.endGraph();
		}
	}


	/**
	 * One state in a path through the Fst. We use this to build the NFAs we shuffle to the partitions
	 *
	 */
	private class PathState {
		protected final int id;
		protected int mergedInto;
		protected boolean isFinal = false;
		protected int writtenAtPos = -1;
		protected int level;
		protected OutputNFA nfa;


		/** Forward pointers */
		protected Long2ObjectSortedMap<PathState> outTransitions;

		/** Backward pointer (in the tree, each state has only one incoming transition) */
		protected PathState predecessor;

		protected PathState(OutputNFA nfa, PathState from, int level) {
			this.nfa = nfa;
			id = nfa.numPathStates++;
			mergedInto = id;
			nfa.pathStates.add(this);
			outTransitions = new Long2ObjectAVLTreeMap<>();

			predecessor = from;
			this.level = level;

			nfa.leafs.add(this.id);
		}

		/**
		 * Starting from this state, follow a given transition with given input item. Returns the state the transition
		 * leads to.
		 *
		 * @param outKey	the outKey to follow
		 * @param nfa       the nfa the path is added to
		 * @return
		 */
		protected PathState followTransition(long outKey, OutputNFA nfa) {
			counterFollowTransitionCalls++;

			// if we have a tree branch with this transition already, follow it and return the state it leads to
			if(outTransitions.containsKey(outKey)) {
				// return this state
				PathState toState = outTransitions.get(outKey);
				return toState;
			} else {
				// if we don't have a path starting with that transition, we create a new one
				//   (unless it is the last transition, in which case we want to transition to the global end state)
				PathState toState;
				// if we merge suffixes, create a state with backwards pointers, otherwise one without
				toState = new PathState(nfa, this, this.level+1);

				// add the transition and the created state to the outgoing transitions of this state and return the state the transition moves to
				outTransitions.put(outKey, toState);

				if(outTransitions.size() == 1)
					nfa.leafs.remove(this.id);

				counterTransitionsCreated++;
				return toState;
			}
		}

		/** Mark this state as final */
		public void setFinal() {
			this.isFinal = true;
		}

		/**
		 * Determine whether this state is mergeable into the passed target state
		 * @param target
		 * @return
		 */
		public boolean isMergeableInto(PathState target) {
			counterIsMergeableIntoCalls++;

			if(this.isFinal != target.isFinal)
				return false;

			if(this.outTransitions.size() != target.outTransitions.size())
				return false;

			ObjectIterator<Long2ObjectMap.Entry<PathState>> aIt = this.outTransitions.long2ObjectEntrySet().iterator();
			ObjectIterator<Long2ObjectMap.Entry<PathState>> bIt = target.outTransitions.long2ObjectEntrySet().iterator();
			Long2ObjectMap.Entry<PathState> a, b;

			// the two states have the same number of out transitions (checked above)
			// now we only check whether those out transitions have the same outKey and point towards the same state

			while(aIt.hasNext()) {
				a = aIt.next();
				b = bIt.next();

				if(a.getLongKey() != b.getLongKey())
					return false;

				if(a.getValue().mergedInto != b.getValue().mergedInto)
					return false;
			}

			return true;
		}


		/** Serialize this state, appending to the given list */
		protected void serializeForward(IntList send) {

			counterSerializedStates++;
			int inpItem;
			int trId;
			long outKey;
			PathState toState;

			writtenAtPos = send.size();

			if(outTransitions.size() > maxNumOutTrs) maxNumOutTrs = outTransitions.size();

			for(Long2ObjectMap.Entry<PathState> entry : outTransitions.long2ObjectEntrySet()) {
				toState = entry.getValue();

                counterSerializedTransitions++;
                // get the state this transition goes to after merging (might be a merged state)

                outKey = entry.getLongKey();
                inpItem = PrimitiveUtils.getLeft(outKey);

                // we write the correct order: (input, [trId], toState)
                if(inpItem < 0) {
                    send.add(inpItem);
                } else {
                    send.add(inpItem);
                    trId = PrimitiveUtils.getRight(outKey);
                    send.add(trId);
                }

                // write toState id and note down that we have written a state id here (which we will convert later)
                nfa.positionsWithStateIds.set(send.size());
                send.add(toState.mergedInto);
			}

			// write state end symbol
			if(isFinal)
				send.add(Integer.MIN_VALUE); // = transitions for this state end here and this is a final state (remeber, we serialize in reversed order)
			else
				send.add(Integer.MAX_VALUE); // = transitions for this state end here, state is not final
		}
	}
}