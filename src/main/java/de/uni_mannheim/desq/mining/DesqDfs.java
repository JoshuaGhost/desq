package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.util.PropertiesUtils;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;

import java.util.BitSet;
import java.util.Iterator;

import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.ItemState;
import de.uni_mannheim.desq.fst.Transition;

public class DesqDfs extends InMemoryDesqMiner {

	
	// parameters for mining
	Fst fst;
	long sigma;
	
	// helper variables
	int[] flist;
	int[] inputSequence;
	boolean reachedFinalState;
	int dfsLevel = 0;
	
	public DesqDfs(DesqMinerContext ctx) {
		super(ctx);
		this.fst = ctx.fst;
		this.sigma = PropertiesUtils.getLong(ctx.properties, "minSupport");
		this.flist = ctx.dict.getFlist().toIntArray();
	}
	
	public void clear() {
		inputSequences.clear();
	}

	@Override
	public void mine() {
		Node root = new Node(null, 0);
		for(int sid = 0; sid < inputSequences.size(); ++sid) {
			inputSequence = inputSequences.get(sid);
			incStep(sid, 0, fst.getInitialState().getStateId(), root);
		}
		
		final IntIterator it = root.children.keySet().iterator();
		while(it.hasNext()) {
			int itemId = it.nextInt();
			Node child = root.children.get(itemId);
			if(child.prefixSupport >= sigma) {
				expand(child);
			}
			child.clear();
		}
		root.clear();
		
		clear();
	}
	
	private void expand(Node node) {
		dfsLevel++;
		int support = 0;
		PostingList.Decompressor projectedDatabase = new PostingList.Decompressor(node.projectedDatabase);
	
		// For all sequences in projected database
		do {
			int sid = projectedDatabase.nextValue();
			inputSequence = inputSequences.get(sid);
			reachedFinalState = false;

			// For all state@pos for a sequence
			do {
				int stateId = projectedDatabase.nextValue();
				int pos = projectedDatabase.nextValue();
				
				// for each T[pos@state]
				incStep(sid, pos, stateId, node);

			} while (projectedDatabase.hasNextValue());

			// increment support if atleast one snapshop corresponds to final state
			if (reachedFinalState) {
				support++;
			}

		} while (projectedDatabase.nextPosting());
		
		// Output if P-frequent
		if (support >= sigma) {
			
			if (ctx.patternWriter != null) {
				// compute output sequence
				int[] outputSequence = new int[dfsLevel];
				int size = dfsLevel;
				
				outputSequence[--size] = node.suffixItemId;
				Node parent = node.parent;
				while(parent.parent != null) {
					outputSequence[--size] = parent.suffixItemId;
					parent = parent.parent;
				}
				//TODO: think of a different way!
				//ctx.patternWriter.write(outputSequence, support);
				ctx.patternWriter.write(new IntArrayList(outputSequence), support);
				//System.out.println(Arrays.toString(outputSequence) + " : " + support);
			}
		}
		
		// Expand children with sufficient prefix support
		final IntIterator it = node.children.keySet().iterator();
		while (it.hasNext()) {
			int itemId = it.nextInt();
			Node child = node.children.get(itemId);
			if (child.prefixSupport >= sigma) {
				expand(child);
			}
			child.clear();
		}
		node.clear();

		dfsLevel--;
	}
	
	private void incStep(int sid, int pos, int stateId, Node node) {
		reachedFinalState |= fst.getState(stateId).isFinal();
		if(pos == inputSequence.length)
			return;
		int itemFid = inputSequence[pos];
		
		//TODO: reuse iterators!
		Iterator<Transition> it = fst.getState(stateId).consume(itemFid);
		while(it.hasNext()) {
			Transition t = it.next();
			if(t.matches(itemFid)) {
				Iterator<ItemState> is = t.consume(itemFid);
				while(is.hasNext()) {
					ItemState itemState = is.next();
					int outputItemFid = itemState.itemFid;
					
					int toStateId = itemState.state.getStateId();
					if(outputItemFid == 0) { //EPS output
						incStep(sid, pos + 1, toStateId, node);
					} else {
						if(flist[outputItemFid] >= sigma) {
							node.append(outputItemFid, sid, pos + 1, toStateId);
						}
					}
				}
			}
		}
	}
	
	// Dfs tree
		private final class Node {
			int prefixSupport = 0;
			int lastSequenceId = -1;
			int suffixItemId;
			Node parent;
			ByteArrayList projectedDatabase = new ByteArrayList();;
			BitSet[] statePosSet = new BitSet[fst.numStates()];
			Int2ObjectOpenHashMap<Node> children = new Int2ObjectOpenHashMap<Node>();

			Node(Node parent, int suffixItemId) {
				this.parent = parent;
				this.suffixItemId = suffixItemId;

				for (int i = 0; i < fst.numStates(); ++i) {
					statePosSet[i] = new BitSet();
				}
			}

			void flush() {
				for (int i = 0; i < fst.numStates(); ++i) {
					statePosSet[i].clear();
				}
			}

			void append(int itemId, int sequenceId, int position, int state) {
				Node node = children.get(itemId);

				if (node == null) {
					node = new Node(this, itemId);
					children.put(itemId, node);
				}

				if (node.lastSequenceId != sequenceId) {

					if (node.lastSequenceId != -1)
						node.flush();

					/** Add transaction separator */
					if (node.projectedDatabase.size() > 0) {
						PostingList.addCompressed(0, node.projectedDatabase);
					}

					node.lastSequenceId = sequenceId;
					node.prefixSupport++;

					PostingList.addCompressed(sequenceId + 1, node.projectedDatabase);
					PostingList.addCompressed(state + 1, node.projectedDatabase);
					PostingList.addCompressed(position + 1, node.projectedDatabase);

					node.statePosSet[state].set(position);
				} else if (!node.statePosSet[state].get(position)) {
					node.statePosSet[state].set(position);
					PostingList.addCompressed(state + 1, node.projectedDatabase);
					PostingList.addCompressed(position + 1, node.projectedDatabase);
				}
			}

			void clear() {
				projectedDatabase = null;
				statePosSet = null;
				children = null;
			}
		}
}
