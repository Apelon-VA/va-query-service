/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.extension;

import javafx.concurrent.Task;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.maven.AbstractMavenLifecycleParticipant;
import org.codehaus.plexus.component.annotations.Component;
import org.ihtsdo.otf.lookup.contracts.contracts.ActiveTaskSet;
import org.ihtsdo.otf.tcc.build.extension.DatabaseBuildExtension;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.reactfx.EventStreams;
import org.reactfx.Subscription;

import java.time.Duration;
import java.util.Set;

@Component(role = AbstractMavenLifecycleParticipant.class, hint = "cradle")
public class CradleExtension extends DatabaseBuildExtension {

    Subscription tickSubscription;

    public CradleExtension() {
        super("Cradle");
        tickSubscription = EventStreams.ticks(Duration.ofSeconds(10))
                .subscribe(tick -> {
                    Set rawTaskSet = Hk2Looker.get().getService(ActiveTaskSet.class).get();
                    Set<Task<?>> taskSet = (Set<Task<?>>)rawTaskSet;
                    taskSet.stream().forEach((task) -> {
                        tick(task);
                    });
                });
    }
}
