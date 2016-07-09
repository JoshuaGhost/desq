package mining.interestingness;

//import java.util.Arrays;

import fst.OutputLabel;
import fst.XFst;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.HashMap;

import mining.scores.DesqCountScore;
import mining.scores.RankedScoreList;
import mining.statistics.collectors.DesqGlobalDataCollector;
import driver.DesqConfig.Match;

public class OnePassIterativeScored extends DesqCountScored {

	// Buffer to store output sequences
	IntArrayList buffer = new IntArrayList();

	// Hashset to store ancestors
	IntOpenHashSet tempAnc = new IntOpenHashSet();

	int pos = 0;

	int numStates = 0;

	int initialState = 0;
	
	boolean rStrict = false;
	
	class Node {
		int item;

		ObjectArrayList<Node> prefixes;

		Node(int item, ObjectArrayList<Node> suffixes) {
			this.item = item;
			this.prefixes = suffixes;
		}
	}

	ObjectArrayList<Node>[] statePrefix;
	int[] stateList;
	int stateListSize = 0;

	@SuppressWarnings("unchecked")
	public OnePassIterativeScored(double sigma, XFst xfst, DesqCountScore score, HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>> globalDataCollectors, RankedScoreList rankedScoreList, boolean writeOutput, Match match) {
		super(sigma, xfst, score, globalDataCollectors, rankedScoreList, writeOutput, match);
		
		numStates = xfst.numStates();
		statePrefix = (ObjectArrayList<Node>[]) new ObjectArrayList[numStates];
		stateList = new int[numStates];
		initialState = xfst.getInitialState();
		if(match == Match.STRICT || match == Match.RSTRICT) {
			 rStrict = true;
		}
		
	}

	private void reset() {
		pos = 0;
		for (int i = 0; i < numStates; ++i) {
			statePrefix[i] = null;
		}
		stateListSize = 0;
	}

	@Override
	protected void computeMatch() {

		statePrefix[initialState] = new ObjectArrayList<Node>();
		stateList[stateListSize++] = initialState;
		statePrefix[initialState].add(null);

		while (pos < sequence.length) {
			step();
			pos++;
		}
		reset();

	}

	private void step() {
		@SuppressWarnings("unchecked")
		ObjectArrayList<Node>[] nextStatePrefix = (ObjectArrayList<Node>[]) new ObjectArrayList[numStates];
		int[] nextStateList = new int[numStates];
		int nextStateListSize = 0;

		int itemId = sequence[pos];
		

		for (int i = 0; i < stateListSize; i++) {
			int fromState = stateList[i];

			if (xfst.hasOutgoingTransition(fromState, itemId)) {
				for (int tId = 0; tId < xfst.numTransitions(fromState); ++tId) {
					if (xfst.canStep(itemId, fromState, tId)) {
						int toState = xfst.getToState(fromState, tId);
						OutputLabel olabel = xfst.getOutputLabel(fromState, tId);

						
						boolean isFinal = xfst.isFinalState(toState);
						if(rStrict) {
							if (pos == sequence.length -1) {
								isFinal &= true;
							} else{
								isFinal = false;
							}
						}
						
						
						Node node;

						if (null == nextStatePrefix[toState]) {
							nextStatePrefix[toState] = new ObjectArrayList<Node>();
							nextStateList[nextStateListSize++] = toState;
						}

						switch (olabel.type) {
						case EPSILON:
							if (isFinal)
								computeOutput(statePrefix[fromState]);
								
							for (Node n : statePrefix[fromState])
								nextStatePrefix[toState].add(n);
							break;

						case CONSTANT:
							int outputItemId = olabel.item;
							if (score.getMaxScoreByItem(outputItemId, globalDataCollectors) >= sigma) {
								node = new Node(outputItemId, statePrefix[fromState]);
								
								if (isFinal)
									computeOutput(node);
								
								if(score.getMaxScoreByPrefix(getCurrentPrefix(node, null), globalDataCollectors) >= sigma) {
									nextStatePrefix[toState].add(node);
								}
							}
							break;

						case SELF:
							if (score.getMaxScoreByItem(itemId, globalDataCollectors) >= sigma) {
								node = new Node(itemId, statePrefix[fromState]);
								
								if (isFinal)
									computeOutput(node);
								
								if(score.getMaxScoreByPrefix(getCurrentPrefix(node, null), globalDataCollectors) >= sigma) {
									nextStatePrefix[toState].add(node);
								}
							}
							break;

						case SELFGENERALIZE:
							IntArrayList stack = new IntArrayList();
							int top = 0;
							int rootItemId = olabel.item;
							stack.add(itemId);
							tempAnc.add(itemId);
							while (top < stack.size()) {
								int currItemId = stack.getInt(top);
								for (int parentId : dictionary.getParents(currItemId)) {
									if (xfst.isReachable(rootItemId, parentId) && !tempAnc.contains(parentId)) {
										stack.add(parentId);
										tempAnc.add(parentId);
									}
								}
								top++;
							}
							tempAnc.clear();
							for (int id : stack) {
								if(score.getMaxScoreByItem(id, globalDataCollectors) >= sigma) {
									node = new Node(id, statePrefix[fromState]);
									if (isFinal)
										computeOutput(node);
									
									if(score.getMaxScoreByPrefix(getCurrentPrefix(node, null), globalDataCollectors) >= sigma) {
										nextStatePrefix[toState].add(node);
									}
								}
							}

							break;
						default:
							break;
						}
					}

				}
			}
		}
		this.stateList = nextStateList;
		this.stateListSize = nextStateListSize;
		this.statePrefix = nextStatePrefix;
	}

	private void computeOutput(ObjectArrayList<Node> suffixes) {
		for (Node node : suffixes)
			computeOutput(node);
	}

	private void outputBuffer() {

		if (!buffer.isEmpty()) {
			updateFinalSequenceStatistics(reverse(buffer.toIntArray()));
//			countSequence(reverse(buffer.toIntArray()));
			// System.out.println(buffer);
		}
	}

	private void computeOutput(Node node) {
		if (node == null) {
			outputBuffer();
			return;
		}

		buffer.add(node.item);
		for (Node n : node.prefixes) {
			computeOutput(n);
		}
		buffer.remove(buffer.size() - 1);
	}
	
	private int[] getCurrentPrefix(Node node, IntArrayList prefix) {
		
		if(prefix == null) {
			prefix = new IntArrayList();
		}
		
		if (node == null) {
			return reverse(prefix.toIntArray());
		}

		prefix.add(node.item);
		for (Node n : node.prefixes) {
			getCurrentPrefix(n,prefix);
		}
		
		return null;
	}

	private int[] reverse(int[] a) {
		int i = 0;
		int j = a.length - 1;
		while (j > i) {
			a[i] ^= a[j];
			a[j] ^= a[i];
			a[i] ^= a[j];
			i++;
			j--;
		}
		// System.out.println(Arrays.toString(a));
		return a;
	}
}