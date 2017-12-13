package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.fst.graphviz.AutomatonVisualizer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntListIterator;
import org.apache.commons.io.FilenameUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PatriciaItemTrie {

    private TrieNode root;
    public static int nodeCounter = 0;

    public PatriciaItemTrie() {
        //init root node with empty list
        this.root = new TrieNode(new IntArrayList(), (long) 0);
    }

    public TrieNode getRoot(){
        return root;
    }

    public void addItems(IntList fids) {
        addItems(fids, (long) 1);
    }

    public void addItems(IntList fids, Long support) {
        //traverse trie and inc support till nodes do not match or anymore or end of list
        IntListIterator it = fids.iterator();
        TrieNode currentNode = root;

        while (it.hasNext()) {
            int currentItem = it.next();
            // Compare input with existing items in node
            if (currentNode.itemIterator().hasNext()) {
                //compare next item in current node
                int nodeItem = currentNode.itemIterator().next();
                if (currentItem != nodeItem) {
                    //node item and input item differ -> split node and extend with remaining!
                    splitNode(currentNode, nodeItem);
                    currentNode.setFinal(false); //is only subsequence
                    //and add new node with remaining input items
                    expandTrie(currentNode, createIntList(currentItem, it),support);
                    break; //remaining input added -> finished processing
                } //else: same item in input and in node so far
                //check if item list is shorter than node Items -> split
                if(!it.hasNext() && currentNode.itemIterator().hasNext()){
                    splitNode(currentNode, currentNode.itemIterator().next());
                }
            //Case: more input items than items in node -> children string with item or expand
            } else {
                //try to get child node starting with item
                TrieNode nextNode = currentNode.getChildrenStartingWith(currentItem);
                if (nextNode == null) {
                    //no next node starting with input item -> expand with new node containing all remaining
                    expandTrie(currentNode, createIntList(currentItem, it), support);
                    break; //remaining input added -> finished processing
                } else {
                    //found child node starting with current item
                    //go to next node, but inc support and clean up current
                    currentNode.incSupport(support);
                    currentNode.clearIterator();
                    currentNode = nextNode;
                    //skip the first item (already checked)
                    currentNode.itemIterator().next();
                    //continue;
                }
            }
        }
        //inc support of last visited node and clean up
        //doing it after loop ensures that the last node is considered if there was no expand
        currentNode.incSupport(support);
        currentNode.clearIterator();
    }

    //Construct the remaining items (TODO: more efficient way?)
    private IntList createIntList(int firstItem, IntListIterator remainingItems) {
        IntList items = new IntArrayList();
        items.add(firstItem);
        if (remainingItems.hasNext()) { //there are more items to add
            remainingItems.forEachRemaining(items::add);
        }
        return items;
    }

    private TrieNode expandTrie(TrieNode startNode, IntList items) {
        return expandTrie(startNode, items, (long) 1);
    }

    private TrieNode expandTrie(TrieNode startNode, IntList items, Long support) {
        //Create new node
        TrieNode newNode = new TrieNode(items, support);
        //Set pointer in parent node
        startNode.addChild(newNode);
        return newNode;
    }

    /**
     * Split node at given separator.
     * Splits an existing node by altering the existing such that it keeps items
     * and parents do not need to be adjusted. The remaining Items are moved to a new node
     * which is attached as child node to the existing
     * @param node to be split
     * @param separatorItem first item of new (second) node
     * @return
     */
    private TrieNode splitNode(TrieNode node, int separatorItem) {
        IntList remaining = node.separateItems(separatorItem);
        //remove children pointer from existing node
        HashMap<Integer, TrieNode> existingChildren = node.removeChildren();
        //expand from existing node with remaining items
        TrieNode newNode = expandTrie(node, remaining, node.support);
        newNode.setFinal(node.isFinal);
        //add existing children to new node
        if (!existingChildren.isEmpty()) {
            newNode.addChildren(existingChildren);
        }
        return newNode;
    }

    /**
     * Exports the trie using graphviz (type bsed on extension, e.g., "gv" (source file), "pdf", ...)
     */
    public void exportGraphViz(String file, Dictionary dict, int maxDepth) {
        AutomatonVisualizer automatonVisualizer = new AutomatonVisualizer(FilenameUtils.getExtension(file), FilenameUtils.getBaseName(file));
        automatonVisualizer.beginGraph();
        expandGraphViz(root, automatonVisualizer, dict,maxDepth);
        //automatonVisualizer.endGraph(trie.getRoot().toString());
        automatonVisualizer.endGraph();
    }

    private void expandGraphViz(TrieNode node, AutomatonVisualizer viz, Dictionary dict, int depth) {
        for (TrieNode child : node.collectChildren()) {
            //viz.add(node.toString(), String.valueOf(child.firstItem()), child.toString());
            viz.add(String.valueOf(node.id), child.toString(dict), String.valueOf(child.id));
            if (child.isFinal) {
                //viz.addFinalState(child.toString());
                viz.addFinalState(String.valueOf(child.id),child.isLeaf);
            }
            if (!child.isLeaf && depth > 0) {
                expandGraphViz(child, viz, dict, depth -1);
            }
        }
    }

    public class TrieNode {
        // unique id
        private int id;
        //The sequence of FIDs
        private IntList items;
        //An iterator (singleton) over the fids
        private IntListIterator  it;
        //The support for this set (beginning at root)
        private Long support;
        //pointers to children
        private HashMap<Integer, TrieNode> children = new HashMap<>();
        private boolean isLeaf; //no children
        private boolean isFinal; //a sequence ends here (instead of summing and comparing support)

        public TrieNode(int item) {
            this(item, (long) 1);
        }

        public TrieNode(int item, Long support) {
            this(support);
            this.items = new IntArrayList(1);
            this.items.add(item);
        }

        public TrieNode(IntList fids) {
            this(fids, (long) 1);
        }

        public TrieNode(IntList fids, Long support) {
            this(support);
            this.items = fids;
        }

        private TrieNode(Long support){
            this.support = support;
            this.isLeaf = true;
            this.isFinal = true;
            this.id = ++nodeCounter;
        }

        // Increment support
        public Long incSupport() {
            return incSupport((long) 1);
        }

        public Long incSupport(Long s) {
            return this.support += s;
        }

        public int firstItem() {
            if (items.isEmpty()) {
                return -1;
            } else {
                return items.getInt(0);
            }
        }

        //returns previous child trie node (if replaced), else null
        public TrieNode addChild(TrieNode t) {
            return addChild(t.firstItem(), t);
        }

        public TrieNode addChild(int firstItem, TrieNode t) {
            if (isLeaf) isLeaf = false;
            //there must be only one child per next fid
            return children.put(firstItem, t);
        }

        public void addChildren(Map<Integer, TrieNode> map) {
            if (isLeaf) isLeaf = false;
            children.putAll(map);
        }

        public Collection<TrieNode> collectChildren() {
            return children.values();
        }

        public HashMap<Integer, TrieNode> removeChildren() {
            HashMap<Integer, TrieNode> removed = children;
            children = new HashMap<>();
            return removed;
        }

        public TrieNode getChildrenStartingWith(int fid) {
            return children.get(fid);
        }

        public IntListIterator itemIterator() {
            if (it == null)
                it = items.iterator();
            return it;
        }

        public IntList getItems(){
            return items;
        }

        public boolean isLeaf(){
            return isLeaf;
        }
        public boolean isFinal(){
            return isFinal;
        }

        public void setFinal(boolean isFinal){
            this.isFinal = isFinal;
        }

        public Long getSupport(){
            return support;
        }

        public void clearIterator() {
            //force re-init of iterator at next call
            it = null;
        }

        public IntList separateItems(int separatorItem) {
            //avoid inconsistent iterator
            clearIterator();
            int idx = items.indexOf(separatorItem);
            //determine to be removed items (for return) and copy them
            IntList removed = new IntArrayList(items.subList(idx, items.size()));
            //delete remaining based on idx
            items.removeElements(idx, items.size());
            return removed;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            boolean first = true;
            //builder.append(id);
            builder.append("[");
            for (int i : items) {
                if (!first) builder.append(",");
                builder.append(i);
                if (first) first = false;
            }
            builder.append("]@");
            builder.append(support);
            return builder.toString();
        }

        public String toString(Dictionary dict) {
            StringBuilder builder = new StringBuilder();
            boolean first = true;
            //builder.append(id);
            builder.append("[");
            for (String s : dict.sidsOfFids(items)) {
                if (!first) builder.append(",");
                builder.append(s);
                if (first) first = false;
            }
            builder.append("]@");
            builder.append(support);
            return builder.toString();
        }
    }
}