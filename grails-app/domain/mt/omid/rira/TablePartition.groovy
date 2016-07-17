package mt.omid.rira

import grails.util.Holders

class TablePartition {

    String partitionName
    String schemaName
    String tableName
    Date valueLessDate
    boolean deleted

    Date dateCreated
    Date lastUpdated

    static constraints = {
        partitionName size: 1..100
        tableName size: 1..100
        schemaName size: 1..100, unique: ['tableName', 'partitionName']
    }

    static mapping = {
        table name: 'r_table_partitions', schema: Holders.grailsApplication.mergedConfig.grails.plugin.rira.schema
    }
}
