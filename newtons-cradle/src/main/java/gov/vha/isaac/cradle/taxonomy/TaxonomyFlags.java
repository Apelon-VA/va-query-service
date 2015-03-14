/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.taxonomy;

import gov.vha.isaac.ochre.api.coordinate.TaxonomyCoordinate;
import java.util.Arrays;
import java.util.EnumSet;

/**
 * An enum of flags used by taxonomy records to indicate if the specified
 * concept is either a parent of, or child of, or other type of relationship.
 * These flags are designed to support a bit representation within the top 8
 * bits of a 32 bit integer (without interfering with the sign bit), enabling
 * multiple flags to be associated with a STAMP value within a single integer.
 *
 * @author kec
 */
public enum TaxonomyFlags {

    STATED(0x10000000),              // 0001 0000
    INFERRED(0x20000000),            // 0010 0000
    SEMEME(0x40000000),              // 0100 0000
    PARENT(0x08000000),              // 0000 1000
    CONCEPT_STATUS(0x04000000),      // 0000 0100
    NON_TAXONOMIC_REL(0x02000000),   // 0000 0010
    RESERVED_FUTURE_USE(0x01000000); // 0000 0001

    public static final int ALL_RELS = 0;
    public static final int PARENT_FLAG_SET = TaxonomyFlags.PARENT.bits;
    public static final int INFERRED_PARENT_FLAGS_SET = TaxonomyFlags.PARENT.bits + TaxonomyFlags.INFERRED.bits;
    public static final int STATED_PARENT_FLAGS_SET = TaxonomyFlags.PARENT.bits +  TaxonomyFlags.STATED.bits;
    public static final int NON_TAXONOMIC_REL_FLAGS_SET = TaxonomyFlags.NON_TAXONOMIC_REL.bits;
    public static final int PARENT_SEMEME_FLAGS_SET = TaxonomyFlags.PARENT.bits + TaxonomyFlags.SEMEME.bits;

    public static int getFlagsFromTaxonomyCoordinate(TaxonomyCoordinate viewCoordinate) {
         switch (viewCoordinate.getTaxonomyType()) {
            case INFERRED:
                return TaxonomyFlags.INFERRED_PARENT_FLAGS_SET;
            case STATED:
                return TaxonomyFlags.STATED_PARENT_FLAGS_SET;
            default:
                throw new UnsupportedOperationException("no support for: " + viewCoordinate.getTaxonomyType());
        }
    }

    public final int bits;

    TaxonomyFlags(int bits) {
        this.bits = bits;
    }
    
    public static int getTaxonomyFlagsAsInt(EnumSet<TaxonomyFlags> flagSet) {
        int flags = 0;
        for (TaxonomyFlags flag: flagSet) {
            flags += flag.bits;
        }
        return flags;
    }
    
    public static EnumSet<TaxonomyFlags> getTaxonomyFlags(int stampWithFlags) {
        EnumSet<TaxonomyFlags> flagSet = EnumSet.noneOf(TaxonomyFlags.class);
        Arrays.stream(TaxonomyFlags.values()).forEach((flag) -> {
            if ((stampWithFlags & flag.bits) == flag.bits) {
                flagSet.add(flag);
            }
        });
        return flagSet;  
    }

}
