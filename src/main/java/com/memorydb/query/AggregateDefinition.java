package com.memorydb.query;

public class AggregateDefinition {
    private String alias;
    private AggregateFunction function;
    private String targetColumn; // Can be null, e.g., for COUNT(*)

    // Default constructor for Jackson or other frameworks if needed
    public AggregateDefinition() {
    }

    public AggregateDefinition(String alias, AggregateFunction function, String targetColumn) {
        this.alias = alias;
        this.function = function;
        this.targetColumn = targetColumn;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public AggregateFunction getFunction() {
        return function;
    }

    public void setFunction(AggregateFunction function) {
        this.function = function;
    }

    public String getTargetColumn() {
        return targetColumn;
    }

    public void setTargetColumn(String targetColumn) {
        this.targetColumn = targetColumn;
    }

    @Override
    public String toString() {
        return "AggregateDefinition{" +
               "alias='" + alias + '\'' +
               ", function=" + function +
               ", targetColumn='" + targetColumn + '\'' +
               '}';
    }
}
