package gov.vha.isaac.cradle.tasks;

import gov.vha.isaac.metadata.coordinates.EditCoordinates;
import gov.vha.isaac.metadata.coordinates.ViewCoordinates;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.ConceptProxy;
import javafx.concurrent.Task;
import org.ihtsdo.otf.tcc.api.blueprint.*;
import org.ihtsdo.otf.tcc.api.coordinate.EditCoordinate;
import org.ihtsdo.otf.tcc.api.refex.RefexType;
import org.ihtsdo.otf.tcc.api.store.Ts;
import org.ihtsdo.otf.tcc.model.cc.termstore.Termstore;
import org.ihtsdo.otf.tcc.model.path.PathManager;

import java.io.IOException;
import java.time.Instant;

/**
 * Created by kec on 2/16/15.
 */
public class AddStampOrigin  extends Task<Void> {


    private ConceptProxy stampPath;
    private ConceptProxy originStampPath;
    private int stampPathNid;
    private int originStampPathNid;
    private Instant originTime;

    public AddStampOrigin(ConceptProxy stampPath, ConceptProxy originStampPath, Instant originTime, Termstore termstore) {
        try {
            this.stampPath = stampPath;
            this.originStampPath = originStampPath;
            updateTitle("Add path origin");
            updateProgress(-1, Long.MAX_VALUE); // Indeterminate progress
            this.stampPathNid = termstore.getNidForUuids(stampPath.getUuids());
            this.originStampPathNid = termstore.getNidForUuids(originStampPath.getUuids());
            this.originTime = originTime;
            PathManager pathManager = PathManager.get();
            if (!pathManager.exists(stampPathNid)) {
                throw new IOException("Unknown to path manager: " + stampPath);
            }
            if (!pathManager.exists(originStampPathNid)) {
                throw new IOException("Unknown to path manager: " + originStampPath);
            }
            if (originTime.isAfter(Instant.now())) {
                throw new IOException("Origins cannot be in the future: " + originTime);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Void call() throws Exception {
        RefexCAB originCAB = new RefexCAB(RefexType.CID_LONG,
                stampPath.getUuids()[0], // referenced component
                IsaacMetadataAuxiliaryBinding.PATH_ORIGINS.getUuids()[0], // origin assemblage
                IdDirective.GENERATE_REFEX_CONTENT_HASH, RefexDirective.INCLUDE);

        originCAB.getProperties().put(ComponentProperty.COMPONENT_EXTENSION_1_ID, originStampPath.getUuids()[0]);
        originCAB.getProperties().put(ComponentProperty.LONG_EXTENSION_1, originTime.getEpochSecond());


        TerminologyBuilderBI builder = Ts.get().getTerminologyBuilder(EditCoordinates.getDefaultUserMetadata(),
                ViewCoordinates.getDevelopmentInferredLatest());
        builder.construct(originCAB);
        Ts.get().commit();

        updateProgress(1, 1);
        updateMessage(String.format("Add path origin complete."));
        return null;
    }
}
