package mt.omid.rira

/**
 * Created by Omid Mehdizadeh on 27/06/2016.
 */
enum PartitionPeriod {
    NO, DAILY, MONTHLY, YEARLY

    public boolean equals(PartitionPeriod p) {
        return this == p
    }

    static PartitionPeriod valueOfName(String name) {
        values().find { it.name() == name }
    }
}