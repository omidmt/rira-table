grails {
    plugin {
        rira-table {
//            appName = 'RIRA'
            schema = 'rira'
//            logoSmall = 'rira-logo-128.png'
//            logoLarg = 'rira-logo-256.png'
//            ntfy
//                    {
//                        smscIP = '127.0.0.1'
//                        smppPort = 2775
//                        sysId = 'sysid'
//                        sysType = 'systype'
//                        sysPass = 'sysPass'
//                        typeOfNumber = 2
//                        npIndicator = 0
//                    }
        }
    }
}

grails.plugin.rira.konfig.plugins.converters = [mt.omid.rira.TableServiceKonfig]

log4j = {
    // Example of changing the log pattern for the default console
    // appender:
    //
    //appenders {
    //    console name:'stdout', layout:pattern(conversionPattern: '%c{2} %m%n')
    //}

    error  'org.codehaus.groovy.grails.web.servlet',  //  controllers
           'org.codehaus.groovy.grails.web.pages', //  GSP
           'org.codehaus.groovy.grails.web.sitemesh', //  layouts
           'org.codehaus.groovy.grails.web.mapping.filter', // URL mapping
           'org.codehaus.groovy.grails.web.mapping', // URL mapping
           'org.codehaus.groovy.grails.commons', // core / classloading
           'org.codehaus.groovy.grails.plugins', // plugins
           'org.codehaus.groovy.grails.orm.hibernate', // hibernate integration
           'org.springframework',
           'org.hibernate',
           'net.sf.ehcache.hibernate'
}
