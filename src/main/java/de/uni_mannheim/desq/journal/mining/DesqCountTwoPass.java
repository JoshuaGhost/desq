package de.uni_mannheim.desq.journal.mining;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.ItemState;
import de.uni_mannheim.desq.fst.State;
import de.uni_mannheim.desq.fst.Transition;
import de.uni_mannheim.desq.journal.edfa.ExtendedDfa;
import de.uni_mannheim.desq.mining.DesqMiner;
import de.uni_mannheim.desq.mining.DesqMinerContext;
import de.uni_mannheim.desq.patex.PatEx;
import de.uni_mannheim.desq.util.PrimitiveUtils;
import de.uni_mannheim.desq.util.PropertiesUtils;


/**
 * DesqCountTwoPass.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */

// TODO: integrate into DesqCount
public class DesqCountTwoPass extends DesqMiner {

	// parameters for mining
	String patternExpression;
	long sigma;
	boolean useFlist = true;
	
	// helper variables
	int largestFrequentFid;
	Fst fst;
	int initialStateId;
	int sid;
	IntList buffer;
	IntList inputSequence;
	Object2LongMap<IntList> outputSequences = new Object2LongOpenHashMap<>();
	ExtendedDfa eDfa;
	ArrayList<Iterator<ItemState>> itemStateIterators = new ArrayList<>();

	// variables for two pass
	BitSet[] posStateIndex;
	IntList finalPos;
	IntList finalStateIds;
	BitSet initBitSet = new BitSet(1);
	
	public DesqCountTwoPass(DesqMinerContext ctx) {
		super(ctx);
		this.sigma = PropertiesUtils.getLong(ctx.properties, "minSupport");
		if(PropertiesUtils.isSet(ctx.properties, "useFlist"))
			this.useFlist = PropertiesUtils.getBoolean(ctx.properties, "useFlist");
		this.largestFrequentFid = ctx.dict.getLargestFidAboveDfreq(sigma);
		this.sid = 0;
		
		this.patternExpression = PropertiesUtils.get(ctx.properties, "patternExpression");
		patternExpression = ".* [" + patternExpression.trim() + "]";
		PatEx p = new PatEx(patternExpression, ctx.dict);
		this.fst = p.translate();
		fst.minimize();//TODO: move to translate
		this.initialStateId = fst.getInitialState().getId();
		
		buffer = new IntArrayList();
		
		this.eDfa = new ExtendedDfa(fst, ctx.dict);
		finalPos = new IntArrayList();
		
		finalStateIds = new IntArrayList();
		for(State state : fst.getFinalStates()) {
			finalStateIds.add(state.getId());
		}
		//create reverse fst
		fst.reverse(false);
		
		//TODO: this always assumes that initialStateId is 0!
		initBitSet.set(0);
	}
	
	private void clear() {
		finalPos.clear();
	}
	
	public static Properties createProperties(String patternExpression, int sigma) {
		Properties properties = new Properties();
		PropertiesUtils.set(properties, "patternExpression", patternExpression);
		PropertiesUtils.set(properties, "minSupport", sigma);
		return properties;
	}
	
	@Override
	protected void addInputSequence(IntList inputSequence) {
		// Make forward pass to compute reachability
		posStateIndex = new BitSet[inputSequence.size() + 1];
		posStateIndex[0] = initBitSet;
		if(eDfa.computeReachability(inputSequence, 0, posStateIndex, finalPos)) {
			this.inputSequence = inputSequence;
			for(int pos : finalPos) {
				for(int stateId : finalStateIds) {
					if(posStateIndex[pos+1].get(stateId)) {
						stepBack(pos, stateId, 0);
					}
				}
			}
			sid++;
			clear();
		}
	}

	@Override
	public void mine() {
		for(Map.Entry<IntList, Long> entry : outputSequences.entrySet()) {
			long value = entry.getValue();
			int support = PrimitiveUtils.getLeft(value);
			if(support >= sigma) {
				if (ctx.patternWriter != null) 
					ctx.patternWriter.write(entry.getKey(), support);
			}
		}
	}
	
	private void stepBack(int pos, int stateId, int level) {
		if(pos < 0) {
			if(!buffer.isEmpty())
				countSequence(buffer);
			return;
		}
		int itemFid = inputSequence.getInt(pos);
		
		for(Transition transition : fst.getState(stateId).getTransitions()) {
			int toStateId = transition.getToState().getId();
			if(transition.matches(itemFid) && posStateIndex[pos].get(toStateId)) {
				// create a new iterator or reuse existing one
				Iterator<ItemState> itemStateIt;
				if(level >= itemStateIterators.size()) {
					itemStateIt = transition.consume(itemFid);
					itemStateIterators.add(itemStateIt);
				} else {
					itemStateIt = transition.consume(itemFid, itemStateIterators.get(level));
				}
				while(itemStateIt.hasNext()) {
					int outputItemFid = itemStateIt.next().itemFid;
					if(outputItemFid == 0)
						stepBack(pos-1, toStateId, level + 1);
					else {
						if(!useFlist || largestFrequentFid >= outputItemFid) {
							buffer.add(outputItemFid);
							stepBack(pos -1, toStateId, level + 1);
							buffer.remove(buffer.size() - 1);
						}
					}
				}
			}
		}
	}
	
	private void countSequence(IntList sequence) {
		Long supSid = outputSequences.get(sequence);
		if (supSid == null) {
			outputSequences.put(new IntArrayList(sequence), PrimitiveUtils.combine(1, sid)); // need to copy here
			return;
		}
		if (PrimitiveUtils.getRight(supSid) != sid) {
		    // TODO: can overflow
		    // if chang order: newCount = count + 1 // no combine
			int newCount = PrimitiveUtils.getLeft(supSid) + 1;
			outputSequences.put(sequence, PrimitiveUtils.combine(newCount, sid));
		}
	}
}
