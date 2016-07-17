package mt.omid.rira

import grails.transaction.Transactional
import groovy.time.TimeCategory

import java.sql.ResultSet
import java.sql.ResultSetMetaData
import org.apache.commons.csv.*

import java.sql.SQLException
import java.util.zip.GZIPOutputStream

import static mt.omid.rira.DataConnection.DATASOURCES
import static mt.omid.rira.Konfig.KONFIGS

@Transactional
class TableService {

    public final static String DATE_FORMAT = 'yyyy-MM-dd'
    public final static String DATETIME_FORMAT = 'yyyy-MM-dd HH:mm:ss'

    def dataConnectionService

    def enablePartitioning(String schema, String tableName, String columnName, PartitionPeriod period, String dataSource)
            throws SQLException {
        log.debug( "Partitioning table ${tableName} in ${period} base" )

        try {
            Date beginDate = new Date().clearTime() + 1
            String partName = generatePartitionName(period, beginDate)
            String partRange = generateInitialPartitionRange(period)
            Date endTime = Date.parse(DATE_FORMAT, partRange)
            String q = KONFIGS.enablePartitionDDL.replace('%TABLE_NAME%', tableName)
                    .replace('%PART_NAME%', partName)
                    .replace('%COLUMN_NAME%', columnName)
                    .replace('%PART_RANGE%', partRange)

            dataConnectionService.executeUpdate(q, dataSource)
            createTablePartitionHistory(schema, tableName, partName, endTime)
            return true
        }
        catch( e ) {
            log.error "Partitioning table ${tableName} failed [$e.message]"
            throw e
//            return false
        }
    }

    def disablePartitioning(String schema, String tableName, String dataSource)
            throws SQLException {
        log.debug( "Remove partitioning of table ${tableName}" )

        try {
            String q = KONFIGS.disablePartitionDDL.replace('%TABLE_NAME%', tableName)

            dataConnectionService.executeUpdate(q, dataSource)
            deleteAllTablePartitions(schema, tableName)
            return true
        }
        catch( e ) {
            log.error "Removing partitioning of table ${tableName} failed [$e.message]"
            throw e
//            return false
        }
    }

    /***
     * @param schema
     * @param tableName
     * @param partitionName
     * @param period
     * @return
     */
    def partitionTable(String schema, String tableName, PartitionPeriod period, String dataStoreName) {
        if(!DATASOURCES[ KONFIGS.dataStore ]) {
            throw new IllegalArgumentException('No data source is set or defined')
        }
        log.debug( "Partitioning table ${tableName} on ${period} period" )

        switch (period) {
            case PartitionPeriod.DAILY:
                partitionByDay(schema, tableName, KONFIGS.dayPartitionExtension, dataStoreName)
                break
            case PartitionPeriod.MONTHLY:
                partitionByMonth(schema, tableName, KONFIGS.monthPartitionExtension, dataStoreName)
                break
            case PartitionPeriod.YEARLY:
                partitionByYear(schema, tableName, KONFIGS.yearPartitionExtension, dataStoreName)
                break
        }
    }
    /***
     * Partition the table for the next 7 days [by default]
     * @param tableName
     * @param extDay Number of days to extend partition
     */
    def partitionByDay(String schema, String tableName, int extDay, String dataStoreName) {
        for(i in 1..extDay) {
            try {

                Date beginTime = new Date().clearTime() + i
                String partitionName = generatePartitionName(PartitionPeriod.DAILY, beginTime)
                Date endTime = beginTime + 1

                createTablePartitionHistory(schema, tableName, partitionName, endTime)

                String q = KONFIGS.addPartitionDDL.replace('%TABLE_NAME%', tableName)
                        .replace('%PARTITION_NAME%', partitionName)
                        .replace('%END_TIME%', endTime.format(DATE_FORMAT))

                dataConnectionService.executeUpdate(q, dataStoreName)
            }
            catch(e) {
                log.error "Add new daily partition to ${tableName} failed [$e.message]"
            }
        }
    }

    def partitionByMonth(String schema, String tableName, int extMonth, String dataStoreName) {
        for(i in 1..extMonth) {
            try {
                Date beginTime
                Date endTime
                use(TimeCategory) {
                    def d = new Date()
                    beginTime = (d.clearTime() - d.getAt(Calendar.DAY_OF_MONTH) + 1) + i.month
                    endTime = beginTime + 1.month
                }
                String partitionName = generatePartitionName(PartitionPeriod.MONTHLY, beginTime)

                createTablePartitionHistory(schema, tableName, partitionName, endTime)

                String q = KONFIGS.addPartitionDDL.replace('%TABLE_NAME%', tableName)
                        .replace('%PARTITION_NAME%', partitionName)
                        .replace('%END_TIME%', endTime.format(DATE_FORMAT))

                dataConnectionService.executeUpdate(q, dataStoreName)
            }
            catch(e) {
                log.error "Add new monthly partition to ${tableName} failed [$e.message]"
            }
        }
    }

    def partitionByYear(String schema, String tableName, int extYear, String dataStoreName) {
        for(i in 1..extYear) {
            try {
                Date beginTime
                Date endTime
                use(TimeCategory) {
                    def d = new Date()
                    beginTime = (d.clearTime() - d.getAt(Calendar.DAY_OF_YEAR) + 1) + i.year
                    endTime = beginTime + 1.year
                }
                String partitionName = generatePartitionName(PartitionPeriod.YEARLY, beginTime)

                createTablePartitionHistory(schema, tableName, partitionName, endTime)

                String q = KONFIGS.addPartitionDDL.replace('%TABLE_NAME%', tableName)
                        .replace('%PARTITION_NAME%', partitionName)
                        .replace('%END_TIME%', endTime.format(DATE_FORMAT))

                dataConnectionService.executeUpdate(q, dataStoreName)
            }
            catch(e) {
                log.error "Add new monthly partition to ${tableName} failed [$e.message]"
            }
        }
    }

    def createTablePartitionHistory(String schema, String tableName, String partitionName, Date endTime) {
        TablePartition tp = new TablePartition()
        tp.partitionName = partitionName
        tp.schemaName = schema
        tp.tableName = tableName
        tp.valueLessDate = endTime
        tp.deleted = false
        tp.save(flush: true)
    }

    /***
     * Remove the history of partitioning in TablePartition
     * @param schema
     * @param tableName
     * @param partitionName
     * @return
     */
    def deleteAllTablePartitions(String schema, String tableName) {
        TablePartition.createCriteria().list {
            and {
                eq 'schemaName', schema
                eq 'tableName', tableName
            }
        }.findAll().each { TablePartition tp ->
            tp.delete(flush: true)
        }
    }

    def deletePartition(String schemaName, String tableName, String partitionName, String dataStoreName) {
        try {
            String q = KONFIGS.deletePartitionSQL.replace('%TABLE_NAME%', tableName)
                    .replace('%PARTITION_NAME%', partitionName)

            dataConnectionService.executeUpdate(q, dataStoreName)
            TablePartition tp = TablePartition.createCriteria().list {
                and {
                    eq 'schemaName', schemaName
                    eq 'tableName', tableName
                    eq 'partitionName', partitionName
                }
            }.find()
            tp.deleted = true
            tp.save(flush: true)
        }
        catch(e) {
            log.error "Delete partition $partitionName of ${tableName} failed [$e.message]"
        }
    }

    def aggregateTrendData(String tableName, String trendSelect, Date from, Date to, String dataStoreName) {
        try {
            String q = KONFIGS.trendInsert.replace('%TREND_TABLE_NAME%', tableName)
                    .replace('%TREND_SELECT%', trendSelect?.replace('%FROM%', from?.format(DATETIME_FORMAT))
                    .replace('%TO%', to?.format(DATETIME_FORMAT)))

            log.debug("Trend query: [$q]")

            dataConnectionService.executeUpdate(q, dataStoreName)
            return true
        }
        catch(e) {
            log.error "Extracting trend data of ${tableName} failed [$e.message]", e
            return false
        }
    }

    def dumpTableData(String schema, String tableName, String dumpQuery, Date from, Date to, String dataStore, String sqlCondition=null) {
        BufferedWriter writer = null
        try {
            String q = dumpQuery.replace('%TABLE_NAME%', tableName)
                    .replace('%FROM%', from?.format(DATETIME_FORMAT))
                    .replace('%TO%', to?.format(DATETIME_FORMAT))
            if(sqlCondition) {
                q = q.replace('%CONDITIONS%', ' AND ' + sqlCondition)
            }
            else {
                q = q.replace('%CONDITIONS%', '')
            }

            String fileName = "${KONFIGS.dumpFileName.replace('%TABLE_NAME%', tableName).replace('%FROM%', from?.format("yyyyMMdd'T'HHmmss")).replace('%TO%', to?.format("yyyyMMdd'T'HHmmss"))}.gz"
            GZIPOutputStream zip = new GZIPOutputStream(new FileOutputStream(new File("${Konfig.KONFIGS.dumpDirectory}/$fileName")))
            writer = new BufferedWriter(new OutputStreamWriter(zip, "UTF-8"))

            CSVFormat cf = CSVFormat.DEFAULT.withRecordSeparator(KONFIGS.dumpRecordSeparator)
                    .withDelimiter(KONFIGS.dumpDelimiter as char)
                    .withQuote('"' as char)
                    .withAllowMissingColumnNames(true)
                    .withQuoteMode(QuoteMode.MINIMAL)

            CSVPrinter cp = new CSVPrinter(writer, cf)

            dataConnectionService.fetchLargeData(q, dataStore) { ResultSet rs ->
                ResultSetMetaData rsmd = rs.getMetaData()
                int columnCount = rsmd.getColumnCount()

                def header = []
                for (int i = 1; i <= columnCount; i++) {
                    header << rsmd.getColumnLabel(i)
                }

                cp.printRecord(header)
                cp.printRecords(rs)
            }
        }
        catch(e) {
            log.error("Dumping table $tableName data failed [$e.message]")
            return false
        }
        finally {
            writer?.close()
        }
    }

    /***
     * This method clean the table according to its partitioning status
     * if no partitioning is set for it, it will delete the mached rows in the period
     * otherwise try to delete matched partition
     * @param srcTblName
     * @param partitionPeriod
     * @param histPeriod
     * @param from
     * @param to
     * @param dataStore
     * @return
     */
    def clearHistoryData(String schemaName, String tableName, PartitionPeriod partitionPeriod, Date from, Date to, String dataStore) {
        if (partitionPeriod == PartitionPeriod.NO) {
            log.debug("Clearing unpartitioned table $tableName for period $from to $to")
            deleteHistoryData(tableName, from, to, dataStore)
        } else {
            log.debug("Finding & deleting partitions of $schemaName.$tableName for period $from to $to")
//            String partName = generatePartitionName(partitionPeriod, from)

            TablePartition.createCriteria().list {
                eq 'schemaName', schemaName
                eq 'tableName', tableName
                lt 'valueLessDate', to
                ge 'valueLessDate', from
                eq 'deleted', false
            }.findAll().each { TablePartition tp ->
                //deletePartition(tableName, partName, from, to, dataStore)
                log.debug "Deleting partition $tp.partitionName"
                deletePartition(tp.schemaName, tp.tableName, tp.partitionName, dataStore)
            }
        }
    }

    /***
     * Delete table data in the from and to range
     * @param tableName
     * @param from
     * @param to
     * @param dataStore
     * @return
     */
    def deleteHistoryData(String tableName, Date from, Date to, String dataStore) {
        try {
            String q = KONFIGS.clearHistoryDataSQL.replace('%TABLE_NAME%', tableName)
                    .replace('%FROM%', from?.format(DATETIME_FORMAT))
                    .replace('%TO%', to?.format(DATETIME_FORMAT))

            dataConnectionService.executeUpdate(q, dataStore)
        }
        catch(e) {
            log.error "Deleting data of ${tableName} failed [$e.message]"
        }
    }

    def isTableExist(String tableName, String dataSource) {

        def rs = DATASOURCES[dataSource].connection?.metaData?.getTables( null, null, tableName, null )

        if( rs?.next() ) {
            return true
        }
        else {
            return false
        }
    }

    private String generatePartitionName(PartitionPeriod period, Date date) {
        switch (period) {
            case PartitionPeriod.DAILY:
                return 'DP' + date.format('YYYY_MM_dd')
            case PartitionPeriod.MONTHLY:
                return 'MP' + date.format('YYYY_MM')
            case PartitionPeriod.YEARLY:
                return 'YP' + date.format('YYYY')
            default:
                log.error("Unknown period reported for partition name generation: $period")
                throw new IllegalArgumentException("Unknown period reported for partition name generation: $period")
        }
    }

    private static String generateInitialPartitionRange(PartitionPeriod p) {
        switch(p) {
            case PartitionPeriod.DAILY:
                use(TimeCategory) {
                    return "${(new Date() + 2.day).format( 'Y-MM-dd' )}"
                }
                break

            case PartitionPeriod.MONTHLY:
                use(TimeCategory) {
                    "${(new Date() + 1.month).format( 'Y-MM' )}-01"
                }
                break

            case PartitionPeriod.YEARLY:
                use(TimeCategory) {
                    "${(new Date() + 1.year).format( 'Y' )}-01-01"
                }
                break

            default:
                return "2100-01-01"
        }
    }
}
