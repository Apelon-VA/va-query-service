package org.ihtsdo.otf.query.implementation;

import javax.xml.bind.*;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;

/**
 * Created by kec on 10/30/14.
 */
public class QuerySerializer {
    public static String marshall(Query q) throws JAXBException, IOException {
        JAXBContext ctx = JaxbForQuery.get();
        q.setup();
        Marshaller marshaller = JaxbForQuery.get().createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.setEventHandler(new ValidationEventHandler() {
            @Override
            public boolean handleEvent(ValidationEvent event) {
                System.out.println(event);
                return true;
            }
        });
        StringWriter builder = new StringWriter();
        System.out.println("Let start: ");

        //builder.append("let\n");
        //System.out.println("Let: " + q.getLetDeclarations());
        marshaller.marshal(q, builder);
        //builder.append("where\n");
        //System.out.println("where: " + q.Where());
        //marshaller.marshal(q.Where(), builder);
        return builder.toString();
    }

    public static Query unmarshall(Reader xmlData) throws JAXBException {
        System.out.println("Unmarshall start: ");
        JAXBContext ctx = JaxbForQuery.get();

        Unmarshaller unmarshaller = ctx.createUnmarshaller();
        unmarshaller.setEventHandler(new ValidationEventHandler() {
            @Override
            public boolean handleEvent(ValidationEvent event) {
                System.out.println(event);
                return true;
            }
        });
        Query query = (Query) unmarshaller.unmarshal(xmlData);

        Marshaller marshaller = ctx.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.marshal(query, System.out);
        return query;
    }
}
