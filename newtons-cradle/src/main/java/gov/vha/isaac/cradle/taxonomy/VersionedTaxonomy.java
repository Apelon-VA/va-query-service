/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.taxonomy;

import gov.vha.isaac.cradle.version.ViewPoint;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.mahout.math.map.OpenIntObjectHashMap;

/**
 *
 * @author kec
 */
public abstract class VersionedTaxonomy {
    
    OpenIntObjectHashMap<int[]> taxonomyRecords;
    ViewPoint viewPoint;

    public VersionedTaxonomy(OpenIntObjectHashMap<int[]> taxonomyRecords, ViewPoint viewPoint) {
        this.taxonomyRecords = taxonomyRecords;
        this.viewPoint = viewPoint;
    }
    
    public abstract IntStream findRoots(ViewPoint viewPoint);
    
    public abstract Stream<IntStream> findPaths(int originSequence, int destinationSequence);
}
