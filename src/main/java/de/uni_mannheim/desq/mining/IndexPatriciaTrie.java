package de.uni_mannheim.desq.mining;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
public class IndexPatriciaTrie {

    private int rootId;  //index of root
    private int size;

    //unchecked array handling!
    private long[] nodeSupport;
    private IntList[] nodeChildren;
    private IntList[] nodeItems;
    //private boolean[] nodeIsFinal;
    private boolean[] nodeIsLeaf;
    private IntervalNode[] nodeInterval;
    private int[] nodeItemsSize;


    public IndexPatriciaTrie(int size, int rootId) {
        this.size = size;
        this.rootId = rootId;

        this.nodeSupport = new long[size];
        this.nodeChildren = new IntList[size];
        this.nodeItems = new IntList[size];
        //this.nodeIsFinal = new boolean[size];
        this.nodeIsLeaf = new boolean[size];
        this.nodeInterval = new IntervalNode[size];
        this.nodeItemsSize = new int[size];

    }

    //Copy trie node to index based list based on trie node id
    public void addNode(PatriciaTrieBasic.TrieNode node) {
        //Node Id
        int idx = node.getId();

        //Fill values with corresponding node index
        nodeItems[idx] =  node.items;
        nodeSupport[idx] =  node.support;
        //nodeIsFinal[idx] = node.isFinal;
        nodeIsLeaf[idx] =  node.isLeaf;
        nodeInterval[idx] = node.intervalNode;//new IntervalNode(node.intervalStart, node.intervalEnd, node.support);
        nodeItemsSize[idx] = node.items.size();

        //convert children
        IntList childrenList = new IntArrayList(node.children.size());
        node.children.values().forEach((trieNode) -> childrenList.add(trieNode.getId()));
        this.nodeChildren[idx] = childrenList;
    }

    //getter
    public int getRootId() {
        return rootId;
    }

    public long getSupport(int nodeId) {
        return nodeSupport[nodeId];
    }

    /*
    public boolean isFinal(int nodeId) {
        return nodeIsFinal[nodeId];
    }*/

    public boolean isLeaf(int nodeId) {
        return nodeIsLeaf[nodeId];
    }

    public IntList getItems(int nodeId) {
        return nodeItems[nodeId];
    }

    public int getItemsSize(int nodeId){
        return nodeItemsSize[nodeId];
    }

    public int getItem(int nodeId, int itemIdx) {
        return nodeItems[nodeId].getInt(itemIdx);
    }

    public IntList getChildren(int nodeId) {
        return nodeChildren[nodeId];
    }

    public IntervalNode getIntervalNode(int nodeId) {
        return nodeInterval[nodeId];
    }





    public int size() {
        return size;
    }

    public void clear() {
        this.size = -1;
        this.rootId = -1;

        this.nodeSupport = null;
        this.nodeChildren = null;
        this.nodeItems = null;
        //this.nodeIsFinal = null;
        this.nodeIsLeaf = null;
        this.nodeInterval = null;
    }

    /* Potentially useful outputs -> but pruned only reason to store node-final information...
    public String getNodeString(int nodeId, Dictionary dict) {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        //builder.append(id);
        builder.append("[");
        for (String s : dict.sidsOfFids(getItems(nodeId))) {
            if (!first) builder.append(",");
            builder.append(s);
            if (first) first = false;
        }
        builder.append("]@");
        builder.append(getSupport(nodeId));
        return builder.toString();
    }

    /**
     * Exports the trie using graphviz (type bsed on extension, e.g., "gv" (source file), "pdf", ...)
     *
    public void exportGraphViz(String file, int maxDepth, Dictionary dict) {
        AutomatonVisualizer automatonVisualizer = new AutomatonVisualizer(FilenameUtils.getExtension(file), FilenameUtils.getBaseName(file));
        automatonVisualizer.beginGraph();
        expandGraphViz(rootId, automatonVisualizer, maxDepth, dict);
        //automatonVisualizer.endGraph(trie.getRoot().toString());
        automatonVisualizer.endGraph();
    }

    private void expandGraphViz(int nodeId, AutomatonVisualizer viz, int depth, Dictionary dict) {
        int nextDepth = depth - 1;
        for (int childId : getChildren(nodeId)) {
            //viz.add(node.toString(), String.valueOf(child.firstItem()), child.toString());
            viz.add(String.valueOf(nodeId), getNodeString(childId, dict), String.valueOf(childId));
            if (isFinal(childId)) {
                //viz.addFinalState(child.toString());
                viz.addFinalState(String.valueOf(childId), isLeaf(childId));
            }
            if (!isLeaf(childId) && depth > 0) {
                expandGraphViz(childId, viz, nextDepth, dict);
            }
        }
    }
    */
}