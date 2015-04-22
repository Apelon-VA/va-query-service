/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle;

import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.SystemStatusService;
import java.io.IOException;
import javax.annotation.PostConstruct;
import org.glassfish.hk2.runlevel.RunLevel;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.jvnet.hk2.annotations.Service;

/**
 *
 * @author kec
 */
@Service(name = "Database loader")
@RunLevel(value = 2)
public class DatabaseLoaderService implements DatabaseLoader {
    
    private DatabaseLoaderService() {
        //For HK2
    }
    
    @PostConstruct
    private void startMe() throws IOException {
        try {
            CradleExtensions isaacDb = Hk2Looker.get().getService(CradleExtensions.class);
            isaacDb.loadExistingDatabase();
        }
        catch (Exception e) {
            LookupService.getService(SystemStatusService.class).notifyServiceConfigurationFailure("Database Loader", e);
            throw e;
        }
    }

}
