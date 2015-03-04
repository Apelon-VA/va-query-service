/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.taxonomy.walk;

import org.ihtsdo.otf.tcc.model.cc.concept.ConceptChronicle;

/**
 *
 * @author kec
 */
public class TaxonomyWalkAccumulator {
    public int conceptsProcessed = 0;
    public int connections = 0;
    public int maxConnections = 0;
    public int minConnections = 0;
    public int parentConnections = 0;
    public int statedParentConnections = 0;
    public int inferredParentConnections = 0;
    public double maxDepthSum = 0;
    public double maxDepth = 0;
    public ConceptChronicle maxDepthConcept;
    ConceptChronicle watchConcept = null;

    void combine(TaxonomyWalkAccumulator u) {
        this.conceptsProcessed += u.conceptsProcessed;
        this.connections += u.connections;
        
        this.maxConnections = Math.max(this.maxConnections, u.maxConnections);
        this.minConnections = Math.max(this.minConnections, u.minConnections);

        this.parentConnections += u.parentConnections;
        this.statedParentConnections += u.statedParentConnections;
        this.inferredParentConnections += u.inferredParentConnections;
        this.maxDepthSum += u.maxDepthSum;
        if (u.maxDepth > this.maxDepth) {
            maxDepthConcept = u.maxDepthConcept;
        }
        this.maxDepth = Math.max(this.maxDepth, u.maxDepth);

    }

    @Override
    public String toString() {
        return "TaxonomyWalkAccumulator{" + 
                "conceptsProcessed=" + conceptsProcessed + 
                ", connections=" + connections + 
                ", maxConnections=" + maxConnections + 
                ", minConnections=" + minConnections + 
                ", parentConnections=" + parentConnections + 
                ", statedParentConnections=" + statedParentConnections + 
                ", inferredParentConnections=" + inferredParentConnections + 
                ", maxDepth=" + maxDepth + 
                //" on concept:\n\n" +  maxDepthConcept.toLongString() +
                ", average depth=" + (maxDepthSum/conceptsProcessed) +
                '}';
    }
}
