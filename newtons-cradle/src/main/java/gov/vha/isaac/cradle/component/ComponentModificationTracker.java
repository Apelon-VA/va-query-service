package gov.vha.isaac.cradle.component;

import org.ihtsdo.otf.tcc.api.chronicle.ComponentChronicleBI;
import org.ihtsdo.otf.tcc.model.cc.component.ConceptComponent;
import org.ihtsdo.otf.tcc.model.cc.concept.ModificationTracker;

/**
 * Created by kec on 7/29/14.
 */
public class ComponentModificationTracker implements ModificationTracker {

    private static ComponentModificationTracker singleton = new ComponentModificationTracker();

    public static ComponentModificationTracker get() {
        return singleton;
    }
    @Override
    public void modified(ComponentChronicleBI component) {
        // TODO: hand off to disruptor? Use a blocking queue?
    }

    @Override
    public void modified(ConceptComponent component, long sequence) {

    }
}
