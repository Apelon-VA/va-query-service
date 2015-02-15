/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.extension;

import org.apache.maven.AbstractMavenLifecycleParticipant;
import org.codehaus.plexus.component.annotations.Component;
import org.ihtsdo.otf.tcc.build.extension.DatabaseBuildExtension;

@Component(role = AbstractMavenLifecycleParticipant.class, hint = "cradle")
public class CradleExtension extends DatabaseBuildExtension {

    public CradleExtension() {
        super("Cradle");
    }
}

