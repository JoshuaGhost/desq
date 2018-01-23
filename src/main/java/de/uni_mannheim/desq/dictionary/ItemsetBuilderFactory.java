package de.uni_mannheim.desq.dictionary;

import de.uni_mannheim.desq.util.DesqProperties;

public class ItemsetBuilderFactory extends DefaultBuilderFactory {
    private static final String defaultItemsetSeparator = "/";
    private String itemsetSeparatorSid;

    public ItemsetBuilderFactory(String itemsetSeparatorSid, Dictionary initialDictionary){
        super(initialDictionary);
        this.itemsetSeparatorSid = itemsetSeparatorSid;
    }

    public ItemsetBuilderFactory(String itemsetSeparatorSid){
        this.itemsetSeparatorSid = itemsetSeparatorSid;
    }

    public ItemsetBuilderFactory(Dictionary initialDictionary){
        this(defaultItemsetSeparator, initialDictionary);
    }

    public ItemsetBuilderFactory(){
        this(defaultItemsetSeparator);
    }

    @Override
    public SequenceBuilder createSequenceBuilder(Dictionary dict) {
        return new DefaultItemsetBuilder(dict, itemsetSeparatorSid);
    }

    @Override
    public DictionaryBuilder createDictionaryBuilder() {
        return (initialDictionary == null)
                ? new ItemsetDictionaryBuilder(itemsetSeparatorSid)
                : new ItemsetDictionaryBuilder(initialDictionary, itemsetSeparatorSid);
    }

    /*
    @Override
    public DictionaryBuilder createDictionaryBuilder(Dictionary initialDict) {
        return new ItemsetDictionaryBuilder(initialDict, itemsetSeparatorSid);
    }*/

    @Override
    public DesqProperties getProperties(){
        DesqProperties p = super.getProperties();
        p.setProperty("desq.dataset.itemset.separator.sid",itemsetSeparatorSid);
        return p;
    }

}
