package mt.omid.rira

import groovy.util.logging.Slf4j

import static mt.omid.rira.Konfig.KONFIGS

/**
 * Created by Omid Mehdizadeh on 27/06/2016.
 */
@Slf4j
class TableServiceKonfig {

    static convert() {
        log.info('Converting rira-table Konfig converter')

        KONFIGS.enablePartitionDDL = KONFIGS.enablePartitionDDL ?: """ALTER TABLE %TABLE_NAME%
                                     Partition By Range COLUMNS(`%COLUMN_NAME%`)
                                     ( PARTITION %PART_NAME% VALUES LESS THAN ('%PART_RANGE%'))"""

        KONFIGS.disablePartitionDDL = KONFIGS.disablePartitionDDL ?: """ALTER TABLE %TABLE_NAME% REMOVE PARTITIONING;"""

        KONFIGS.trendInsert = KONFIGS.trendInsert ?: """INSERT IGNORE INTO %TREND_TABLE_NAME% %TREND_SELECT%"""
//"""SELECT CounterId, HostId, date_format( DtTm, '%DATE_GROUP_FORMAT%' ) as DT, SUM(Val), MIN(Val), MAX(Val), Count(Val)
//FROM %TABLE_NAME% WHERE DtTm BETWEEN '%FROM%' AND '%TO%' %COUNTERS_CONDITION%
//Group By HostId, CounterId, DT"""

        KONFIGS.addPartitionDDL = KONFIGS.addPartitionDDL ?: """ALTER TABLE `%TABLE_NAME%`
                                                                ADD PARTITION (PARTITION %PARTITION_NAME% VALUES LESS THAN ('%END_TIME%'));"""

        KONFIGS.deletePartitionSQL = KONFIGS.deletePartitionSQL ?: """ALTER TABLE `%TABLE_NAME%` DROP PARTITION `%PARTITION_NAME%`;"""

        KONFIGS.clearHistoryDataSQL = KONFIGS.clearHistoryDataSQL ?: """DELETE FROM `%TABLE%` WHERE DtTm BETWEEN '%FROM%' AND '%TO%'"""

        KONFIGS.dataDumpDir = KONFIGS.dataDumpDir ?: '.'

        KONFIGS.dumpTableQuery = KONFIGS.dumpTableQuery ?: "Select * FROM %TABLE_NAME% WHERE DtTm BETWEEN '%FROM%' AND '%TO%' %CONDITIONS% "

        KONFIGS.dumpDirectory = KONFIGS.dumpDirectory ?: '.'

        KONFIGS.dumpFileName = KONFIGS.dumpFileName ?: '%TABLE_NAME%-%FROM%-%TO%.csv'

        KONFIGS.dumpDelimiter = KONFIGS.dumpDelimiter ?: ','

        KONFIGS.dumpQuote = KONFIGS.dumpQuote ?: '"'

        KONFIGS.dumpRecordSeparator = KONFIGS.dumpRecordSeparator ?: '\n'
    }

}
