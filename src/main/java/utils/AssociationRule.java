package utils;

public class AssociationRule {
    private double support;
    private double confidence;
    private double lift;
    private boolean isPAR;

    public AssociationRule(double support, double confidence, double lift, boolean isPAR) {
        this.support = (double) Math.round(support * 100) / 100;
        this.confidence = (double) Math.round(confidence * 100) / 100;
        this.lift = (double) Math.round(lift * 100) / 100;
        this.isPAR = isPAR;
    }

    @Override
    public String toString() {
        return (isPAR?"Positive":"Negative")+"AssociationRule{" +
                "support=" + support +
                ", confidence=" + confidence +
                ", lift=" + lift +
                '}';
    }

    public double getSupport() {
        return support;
    }

    public double getConfidence() {
        return confidence;
    }

    public double getLift() {
        return lift;
    }

    public boolean isPAR() {
        return isPAR;
    }
}
