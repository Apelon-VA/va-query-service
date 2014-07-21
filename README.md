# Query Service
## What is it?
The terminology query management module which is one of the components of the IHTSDO Terminology Open Tooling Framework. 

This query implementation module implements queries against the Terminology Component Chronicle service, and is intended to provide a pure java implementation of query capabilities that may be called independently of the REST service, and is also called by the REST service to perform queries.

This module performs tests against a SNOMED CT database and implements a [Jersey 2.2][11] REST service for querying.

This module generates java data display objects derived from running the JAXB xjc against the the underlying implementation. This XML Schema Document is then compiled using JAXB schemagen to generate java files. The .xsd document may be similarly useful for generating JSON for javascript developers. Future versions of the project will provide more complete examples using the JAXB generated data display objects to reduce client dependencies on other libraries.

This project uses [Docbook 5][12] for generating documentation. Docbook enabled distributed editing with configuration mangament, and multiple distribution modalities. Docbook generation is integrated with Maven using [Docbkx Tools][13], and follows in the footsteps of other development projects such as [Sonatype's Maven book][14] and [JBoss's community documentation][15]. More details on how to contribute documentation will come in subsequent versions of this documentation.

The original of this document (with extra examples) can be found at [the OTF documentation website](http://otf.snomedtools.com/query-documentation.html).

Further documentation can be found here - [http://ihtsdo.github.io/OTF-Query-Services/](http://ihtsdo.github.io/OTF-Query-Services/)


## Using the Query Service REST API

This query client module demonstrates how to connect to the query service REST server using an [Apache Jersey 2.2][11] REST client.

There are two example programs: `HelloExample` and `KindOfQueryExample`.

#### HelloExample
The `HelloExample` program, located in the [query-client package](https://github.com/IHTSDO/OTF-Query-Services/tree/master/query-client/src/main/java/org/ihtsdo/otf/query/rest/client/examples), `org.ihtsdo.otf.query.rest.client.examples` is a simple program that sends a hello request to the rest server.

This program is simply to test connectivity and server status. A simple request of `http://api.snomedtools.com/query-service/hello/frank` should return the following to the console:

`200`

`hello frank.`

The java file contains comments on each line to show the setup of the client, sending the request, and processing the result. The program has a main() and therefore can be invoked from within most IDE's by right clicking on the file, and selecting a run or debug option. Please review the java file for more details.

#### QueryExample
The `KindOfQueryExample` program, located in again in the [query-client package](https://github.com/IHTSDO/OTF-Query-Services/tree/master/query-client/src/main/java/org/ihtsdo/otf/query/rest/client/examples), `org.ihtsdo.otf.query.rest.client.examples` performs a simple kind of query using the rest server, and returns data display objects with the results.

The structure of a query is defined by

  * a `ViewCoordinate` that defines what version of the terminology to query against, as well as other information like the preferred language for results.

  * a FOR that defines the set of components to iterate over

  * a LET that defines references to concept specifications or other view coordinates used by where clauses.

  * a WHERE that defined the where clauses for the query

  * a RETURN that defines that type of components to return (Concepts, descriptions, etc).

This example's main method will setup a kind-of query that will return concepts that are kind-of SNOMED "allergic asthma" concepts.

## Building the Query Service
### Query service repository structure

The query service top-level project is the query-parent project that defines the root of the repository structure. The query service repository holds a maven multi-module project manages the sources and documents for the project. Understanding Maven is a prerequisite to understanding how to build and work with the query service. More information on Maven is available at the [Maven][1] web site.

Maven supports project aggregation in addition to project inheritance through its module structure. See [Maven's Guide to Working with Multiple Modules][2] and [Maven's Introduction to the POM][3] for more information about Maven modules, project inheritance, and project aggregation.

Within the top level project are six maven modules (subprojects), some of which are only built when a particular build profile is activated.

  1. **query-client**

    * group id: org.ihtsdo.otf

    * artifact id: query-client

    * directory: query-client

    * build profiles: default, all, query-service, integration-tests, documentation

  2. **query-service**

    * group id: org.ihtsdo.otf

    * artifact id: query-service

    * directory: query-service

    * build profiles: all, query-service, integration-tests, documentation

  3. **query-implementation**

    * group id: org.ihtsdo.otf

    * artifact id: query-implementation

    * directory: query-implementation

    * build profiles: all, query-service, integration-tests, documentation

  4. **query-jaxb-objects**

    * group id: org.ihtsdo.otf

    * artifact id: query-jaxb-objects

    * directory: query-jaxb-objects

    * build profiles: all, query-service, integration-tests, documentation

  5. **query-integration-tests**

    * group id: org.ihtsdo.otf

    * artifact id: query-integration-tests

    * directory: query-integration-tests

    * build profiles: all, integration-tests, documentation

  6. **query-documentation**

    * group id: org.ihtsdo.otf

    * artifact id: query-documentation

    * directory: query-documentation

    * build profiles: all, documentation
    

### Query service build profiles

The query service defines three build profiles described in the following sections. For more information on build profiles, see Maven's[Introduction to build profiles][4].

The default build profile consists of the modules that build when no profile is specifically specified. By default the following modules are built:

This profile will provide a sufficient build to test the query client provided the repository access is properly configured, and can download the Maven XSD artifacts the JAXB Objects requires.

These artifacts should be in the IHTSDO public of the IHTSDO maven repository, and should be able to download automatically by developers who do not have a user account (THIS IS UNTESTED!).

#### Query Service build profile

This profile will build the query service and dependent modules. This project has more dependencies, including dependencies in the 3rd party part of the IHTSDO maven repository which is password protected.

#### Integration tests build profile

The integration tests build profile adds the integration tests module to the build when the build profile id integration-tests is activated. The integration tests are not part of the default build profile because they have an external dependency on a Berkeley SNOMED database that is rather large, and downloading and opening this database may not be necessary for all types of development. Omitting this module from the default build profile makes the default build rapid.

The build time for the integration tests module takes about 1 min 20 sec on a high-spec developers laptop, while the other modules in this project take between 0.5 and 5 seconds.

The documentation build profile adds the integration tests module and the documentation module to the build when the build profile id documentation is activated. Generation of documentation depends of proper execution of the integration tests module, and therefore is removed from the default build profile secondary to the resource requirements and build time of the integration tests module.

Developers must either install Maven to use from the command line, use an IDE that has Maven already integrated (IntelliJ IDEA, Netbeans), or add a plugin to their IDE (Eclipse) that adds Maven support. Instructions on installing Maven for use from the command line are available at Maven's [download site][5]. Integration information for IntelliJ IDEA is available from the Jetbrains wiki for [creating and importing maven projects][6]. Integration information for Netbeans is available from Netbean's [Maven wiki page][7].

To access the artifact dependencies necessary to build the project, the Maven settings.xml file must be appropriately configured. More information about the settings.xml file and it's location is available on Maven's [Settings Reference][8] web page.

Developers will need an account to access the IHTSDO's repository, which can be requested from Rory Davidson. In future releases, the client will be buildable using only a public repository that does not require a user account, but the settings.xml file will still have to be properly configured.

The Query service project can be checked out anonymously from GIT with this command:

`$git clone https://github.com/IHTSDO/OTF-Query-Services.git`

The project can be built from a console with this command:

`$mvn clean install`

This will do a maven build using the default profiles. The settings.xml must be properly configured for this command succeed

Integration tests can be enabled by activating the integration-tests profile, as shown in the following command:

`$mvn clean install -P integration-tests`

This will do a maven build using the default profiles. The settings.xml must be properly configured for this command succeed.

The default build command generates a `.war` file that can be deployed to an app server. Before deployment the app server must be properly configured with adequate memory, and access to the Berkeley database folder. A .zip file of the Berkeley databas foler can be accessed from the [file releases section][9] of the [Open Tooling Framework][10] website.

When the rest server starts (currently at first query), it opens the berkeleydb located in the folder berkeley-db, wherever that relative path is on your server. On my server, it is at:

/Users/kec/GlassFish_Server/glassfish/domains/ttk/config/berkeley-db

(the config directory of the domain appears to be the working directory) You can override the location by setting a system property on the server:


    -Dorg.ihtsdo.otf.tcc.datastore.bdb-location=


It writes diagnostic output when opening the database as follows:



    INFO: QS_sprint2:_Query_rest_service was successfully deployed in 3,637 milliseconds.
    INFO: org.ihtsdo.otf.tcc.datastore.bdb-location not set. Using default location of: berkeley-db
    INFO: setup dbRoot: berkeley-db
    INFO: absolute dbRoot: /Users/kec/GlassFish_Server/glassfish/domains/ttk/config/berkeley-db
    INFO: NidCidMap readOnlyRecords: 812
    INFO: NidCidMap mutableRecords: 0


And takes a few minutes to open. Inside the test data is an old format view coordinate... So it throws a serialization exception:



    SEVERE: java.io.StreamCorruptedException: unexpected block data at
            java.io.ObjectInputStream.readObject0(ObjectInputStream.java:1362) at
            java.io.ObjectInputStream.defaultReadFields(ObjectInputStream.java:1989) at
            java.io.ObjectInputStream.readSerialData(ObjectInputStream.java:1913) at
            java.io.ObjectInputStream.readOrdinaryObject(ObjectInputStream.java:1796)


Just ignore that… It will self heal.


   [1]: http://maven.apache.org
   [2]: http://maven.apache.org/guides/mini/guide-multiple-modules.html
   [3]: http://maven.apache.org/guides/introduction/introduction-to-the-pom.html
   [4]: http://maven.apache.org/guides/introduction/introduction-to-profiles.html
   [5]: http://maven.apache.org/download.cgi
   [6]: http://wiki.jetbrains.net/intellij/Creating_and_importing_Maven_projects
   [7]: http://wiki.netbeans.org/Maven
   [8]: http://maven.apache.org/settings.html
   [9]: https://csfe.aceworkspace.net/sf/frs/do/listReleases/projects.the_ihtsdo_terminology_open_tool/frs.test_data
   [10]: https://csfe.aceworkspace.net/sf/projects/the_ihtsdo_terminology_open_tool/
   [11]: http://jersey.java.net
   [12]: http://www.docbook.org
   [13]: http://code.google.com/p/docbkx-tools/
   [14]: http://blog.sonatype.com/people/2008/04/writing-a-book-with-maven-part-i/
   [15]: https://www.jboss.org/pressgang/jdg
  