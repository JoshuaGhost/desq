package de.uni_mannheim.desq.fst;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class FstOperations {

	private FstOperations() {
	};

	/** Returns an FST that is concatenation of two FSTs */
	public static Fst concatenate(Fst a, Fst b) {
		for (State state : a.getFinalStates()) {
			state.isFinal = false;
			state.simulateEpsilonTransition(b.initialState);
		}
		a.updateStates();
        return a;
	}

	/** Returns an FST that is a union of two FSTs */
	public static Fst union(Fst a, Fst b) {
		State s = new State();
		s.simulateEpsilonTransition(a.initialState);
		s.simulateEpsilonTransition(b.initialState);
		a.initialState = s;
		a.updateStates();
		return a;
	}

	/** Returns an FST that accepts a kleene star of a given FST */
	public static Fst kleene(Fst a) {
		State s = new State();
		s.isFinal = true;
		s.simulateEpsilonTransition(a.initialState);
		for (State p : a.getFinalStates())
			p.simulateEpsilonTransition(s);
		a.initialState = s;
		a.updateStates();
		return a;
	}

	/** Returns an FST that accepts a kleene plus of a given FST */
	public static Fst plus(Fst a) {
		// return concatenate(n, kleene(n));
		for (State s : a.getFinalStates()) {
			s.simulateEpsilonTransition(a.initialState);
		}
		a.updateStates();
		return a;
	}

	/** Returns an FST that accepts zero or one of a given NFA */
	public static Fst optional(Fst a) {
		State s = new State();
		s.simulateEpsilonTransition(a.initialState);
		s.isFinal = true;
		a.initialState = s;
		a.updateStates();
		return a;
	}

	public static Fst repeat(Fst a, int n) {
		if (n == 0) {
		    return new Fst(true);
		}
		Fst[] fstList = new Fst[n - 1];
		for (int i = 0; i < fstList.length; ++i) {
			fstList[i] = a.shallowCopy();
		}
		for (int i = 0; i < fstList.length; ++i) {
			for (State state : a.getFinalStates()) {
				state.isFinal = false;
				state.simulateEpsilonTransition(fstList[i].initialState);
			}
			a.updateStates();
		}
		return a;
	}
	
	public static Fst repeatMin(Fst a, int min) {
		Fst aPlus = plus(a.shallowCopy());
		Fst aMax = repeat(a.shallowCopy(), min - 1);
		return concatenate(aMax, aPlus);
	}
	
	public static Fst repeatMinMax(Fst a, int min, int max) {
		max -= min;
		assert max>=0;
        if (max==0) return new Fst(true);
        Fst fst;
		if (min == 0) {
			fst = new Fst(true);
		} else if (min == 1) {
			fst = a.shallowCopy();
		} else {
			fst = repeat(a.shallowCopy(), min);
		}
		if (max > 0) {
			Fst aa = a.shallowCopy();
			while (--max > 0) {
				Fst ab = a.shallowCopy();
				for (State state : ab.getFinalStates()) {
					state.simulateEpsilonTransition(aa.initialState);
				}
				aa = ab;
			}
			for (State state : fst.getFinalStates()) {
				state.simulateEpsilonTransition(aa.initialState);
			}
		}
		fst.updateStates();
		return fst;
	}
	
	
	public static void minimize(Fst fst) {
		
	}
	
	public static List<State> reverse(Fst fst) {
		return reverse(fst, true);
	}
	
	public static List<State> reverse(Fst fst, boolean createNewInitialState) {
		Map<State, Set<Transition>> reverseTMap = new HashMap<>();

		reverseTMap.put(fst.initialState, new HashSet<Transition>());

		for (State s : fst.states) {
			for (Transition t : s.transitionSet) {
				Set<Transition> tSet = reverseTMap.get(t.toState);
				if (tSet == null) {
					tSet = new HashSet<Transition>();
					reverseTMap.put(t.toState, tSet);
				}
				Transition r = t.shallowCopy();
				r.setToState(s);
				tSet.add(r);
			}
		}

		// Update states with reverse transtitions
		for (State s1 : fst.states) {
			s1.transitionSet = reverseTMap.get(s1);
		}

		if (createNewInitialState) {
			// If we want one initial state
			fst.initialState = new State();
			for (State a : fst.finalStates) {
				fst.initialState.simulateEpsilonTransition(a);
			}
			fst.updateStates();
		}
		return fst.finalStates;
	}
	
}
