package de.uni_mannheim.desq.dictionary;

import it.unimi.dsi.fastutil.ints.IntList;

public class DefaultItemsetBuilder extends DefaultSequenceBuilder{
    private int separatorGid = 0;

    public DefaultItemsetBuilder(Dictionary dict, String separatorSid) {
        super(dict);
        this.separatorGid = dict.gidOf(separatorSid);
    }

    @Override
    public IntList getCurrentGids() {
        IntList gids = super.getCurrentGids();
        //Sort before returning - but change order only if no separator
        gids.sort((g1, g2) -> (
                (g1 == separatorGid || g2 == separatorGid )
                        ? 0 //no change of order
                        : dict.fidOf(g2) - dict.fidOf(g1) //descending
        ));

        //removing duplicates
        int prevGid = -1;
        for(int idx = 0; idx < gids.size(); idx++) {
            if(prevGid == gids.getInt(idx)){
                //remove adjacent duplicate!
                gids.removeInt(idx);
            }else{
                prevGid = gids.getInt(idx);
            }
        }
        return gids;
    }
}
