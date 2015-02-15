/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle;

import gov.vha.isaac.cradle.collections.StampAliasMap;
import gov.vha.isaac.cradle.collections.StampCommentMap;
import gov.vha.isaac.lookup.constants.Constants;
import gov.vha.isaac.ochre.api.chronicle.ChronicledConcept;
import gov.vha.isaac.ochre.api.commit.Alert;
import gov.vha.isaac.ochre.api.commit.ChangeChecker;
import gov.vha.isaac.ochre.api.commit.CommitManager;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import javafx.collections.ObservableList;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.glassfish.hk2.runlevel.RunLevel;
import org.jvnet.hk2.annotations.Service;

/**
 *
 * @author kec
 */
@Service(name = "Cradle Commit Manager")
@RunLevel(value = 1)
public class CradleCommitManager implements CommitManager {
    private static final Logger log = LogManager.getLogger();
    public static final String DEFAULT_CRADLE_COMMIT_MANAGER_FOLDER = "commit-manager";
    private static final String STAMP_ALIAS_MAP_FILENAME = "stamp-alias.map";
    private static final String STAMP_COMMENT_MAP_FILENAME = "stamp-comment.map";

    private final StampAliasMap stampAliasMap = new StampAliasMap();
    private final StampCommentMap stampCommentMap = new StampCommentMap();
    private Path commitManagerFolder;

    
    @PostConstruct
    private void startMe() throws IOException {
        log.info("Starting CradleCommitManager post-construct");
        String issacRootFolder = System.getProperty(Constants.CHRONICLE_COLLECTIONS_ROOT_LOCATION_PROPERTY);
        if (issacRootFolder == null || issacRootFolder.isEmpty()) {
                throw new IllegalStateException(Constants.CHRONICLE_COLLECTIONS_ROOT_LOCATION_PROPERTY + 
                        " has not been set.");
        }
        

        commitManagerFolder = java.nio.file.Paths.get(issacRootFolder, DEFAULT_CRADLE_COMMIT_MANAGER_FOLDER);
        if (Files.exists(commitManagerFolder)) {
           log.info("Loading: " + STAMP_ALIAS_MAP_FILENAME);
           stampAliasMap.read(new File(commitManagerFolder.toFile(), STAMP_ALIAS_MAP_FILENAME));
           log.info("Loading: " + STAMP_COMMENT_MAP_FILENAME);
           stampCommentMap.read(new File(commitManagerFolder.toFile(), STAMP_COMMENT_MAP_FILENAME));
        } else {
            Files.createDirectories(commitManagerFolder);
        }
        
    }
    
    @PreDestroy
    private void stopMe() throws IOException {
        log.info("Stopping CradleCommitManager pre-destroy. ");
        log.info("writing: " + STAMP_ALIAS_MAP_FILENAME);
        stampAliasMap.write(new File(commitManagerFolder.toFile(), STAMP_ALIAS_MAP_FILENAME));
        log.info("writing: " + STAMP_COMMENT_MAP_FILENAME);
        stampCommentMap.write(new File(commitManagerFolder.toFile(), STAMP_COMMENT_MAP_FILENAME));
    }
    
    @Override
    public void addAlias(int stamp, int stampAlias, String aliasCommitComment) {
        stampAliasMap.addAlias(stamp, stampAlias);
        if (aliasCommitComment != null) {
            stampCommentMap.addComment(stampAlias, aliasCommitComment);
        }
    }

    @Override
    public int[] getAliases(int stamp) {
        return stampAliasMap.getAliases(stamp);
    }

    @Override
    public void setComment(int stamp, String comment) {
        stampCommentMap.addComment(stamp, comment);
    }

    @Override
    public Optional<String> getComment(int stamp) {
        return stampCommentMap.getComment(stamp);
    }

    @Override
    public void addUncommitted(ChronicledConcept cc) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void addUncommittedNoChecks(ChronicledConcept cc) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void cancel() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void cancel(ChronicledConcept cc) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void commit(String commitComment) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void commit(ChronicledConcept cc, String commitComment) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ObservableList<Integer> getUncommittedConceptNids() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ObservableList<Alert> getAlertList() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void addChangeChecker(ChangeChecker checker) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void removeChangeChecker(ChangeChecker checker) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    
}
