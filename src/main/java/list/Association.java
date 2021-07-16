package list;

public class Association {
    private ItemSet first,second;
    private boolean firstIsNegated,secondIsNegated;
    private int supportUnion,supportFirst,supportSecond;


    public Association(ItemSet first, ItemSet second, boolean firstIsNegated, boolean secondIsNegated, int supportUnion, int supportFirst, int supportSecond) {
        this.first = first;
        this.second = second;
        this.firstIsNegated = firstIsNegated;
        this.secondIsNegated = secondIsNegated;
        this.supportUnion = supportUnion;
        this.supportFirst = supportFirst;
        this.supportSecond = supportSecond;
    }

    public ItemSet getFirst() {
        return first;
    }

    public ItemSet getSecond() {
        return second;
    }

    public boolean isFirstNegated() {
        return firstIsNegated;
    }

    public boolean isSecondNegated() {
        return secondIsNegated;
    }


    public int getSupportUnion() {
        return supportUnion;
    }

    public int getSupportFirst() {
        return supportFirst;
    }

    public int getSupportSecond() {
        return supportSecond;
    }

    public double getSupport(int numTxn){
        return ((double)supportUnion)/numTxn;
    }
    public double getConfidence(){
        return ((double)supportUnion)/supportFirst;
    }
    public double getLift(int numTxn){
        return ((double)supportUnion*numTxn)/(supportFirst*supportSecond);
    }

    @Override
    public String toString() {
        return (isFirstNegated()?"~ ":"")+first.toString() +
                " => " + (isSecondNegated()?"~ ":"")+second.toString() ;
    }
}
