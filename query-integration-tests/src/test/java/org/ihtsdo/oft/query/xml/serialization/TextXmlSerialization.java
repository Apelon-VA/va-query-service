package org.ihtsdo.oft.query.xml.serialization;

import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.query.implementation.QuerySerializer;
import org.ihtsdo.otf.query.integration.tests.AndTest;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.StringReader;
import java.util.logging.Logger;

/**
 * Created by kec on 10/30/14.
 */
public class TextXmlSerialization {
    @Test
    public void andTest() throws IOException, JAXBException {
//        Assert.assertNotNull(Ts.get(), "Termstore is null");
        Query query = new AndTest().getQuery();
        String queryXML = QuerySerializer.marshall(query);
        System.out.println("1: " + queryXML);

        Query query2 = QuerySerializer.unmarshall(new StringReader(queryXML));

        System.out.println("Query 1 vc: " + ((ViewCoordinate)query.getLetDeclarations().get("Current view coordinate")).getLanguagePreferenceList());
        System.out.println("Query 2 vc: " + ((ViewCoordinate)query2.getLetDeclarations().get("Current view coordinate")).getLanguagePreferenceList());
        String queryXML2 = QuerySerializer.marshall(query2);
        System.out.println("2: " + queryXML2);
        Assert.assertEquals(queryXML2, queryXML);

    }

    public static void main(String[] args) {
//        String DIR = System.getProperty("user.dir");
//        PersistentStoreI PS;
        Logger LOGGER = Logger.getLogger(TextXmlSerialization.class.getName());
//        JFXPanel panel = new JFXPanel();
//        LOGGER.log(Level.INFO, "oneTimeSetUp");
//        System.setProperty(TermstoreConstants.TERMSTORE_LOCATION_PROPERTY, DIR + "/target/test-resources/berkeley-db");
//        RunLevelController runLevelController = Hk2Looker.get().getService(RunLevelController.class);
//        LOGGER.log(Level.INFO, "going to run level 1");
//        runLevelController.proceedTo(1);
//        LOGGER.log(Level.INFO, "going to run level 2");
//        runLevelController.proceedTo(2);
//        PS = Hk2Looker.get().getService(PersistentStoreI.class);
//
//        ConceptChronicleBI concept;
//        try {
//            concept = PS.getConcept(UUID.fromString("2faa9260-8fb2-11db-b606-0800200c9a66"));
//            LOGGER.log(Level.INFO, "WB concept: {0}", concept.toLongString());
//        } catch (IOException ex) {
//            Logger.getLogger(QueryTest.class.getName()).log(Level.SEVERE, null, ex);
//        }

        TextXmlSerialization test = new TextXmlSerialization();
        try {
            test.andTest();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JAXBException e) {
            e.printStackTrace();
        }

//        LOGGER.log(Level.INFO, "oneTimeTearDown");
//        runLevelController = Hk2Looker.get().getService(RunLevelController.class);
//        LOGGER.log(Level.INFO, "going to run level 1");
//        runLevelController.proceedTo(1);
//        LOGGER.log(Level.INFO, "going to run level 0");
//        runLevelController.proceedTo(0);
    }
}
