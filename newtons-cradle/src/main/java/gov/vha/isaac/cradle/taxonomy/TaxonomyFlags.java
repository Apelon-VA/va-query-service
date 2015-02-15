/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.taxonomy;

import java.util.EnumSet;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;

/**
 *
 * @author kec
 */
public enum TaxonomyFlags {
    STATED(0x10000000), INFERRED(0x20000000), PARENT(0x40000000), CHILD(0x08000000);
    public static final EnumSet<TaxonomyFlags> CHILD_FLAG_SET = EnumSet.of(TaxonomyFlags.CHILD);
    public static final EnumSet<TaxonomyFlags> INFERRED_CHILD_FLAGS_SET = EnumSet.of(TaxonomyFlags.CHILD, TaxonomyFlags.INFERRED);
    public static final EnumSet<TaxonomyFlags> PARENT_FLAG_SET = EnumSet.of(TaxonomyFlags.PARENT);
    public static final EnumSet<TaxonomyFlags> INFERRED_PARENT_FLAGS_SET = EnumSet.of(TaxonomyFlags.PARENT, TaxonomyFlags.INFERRED);
    public static final EnumSet<TaxonomyFlags> STATED_CHILD_FLAGS_SET = EnumSet.of(TaxonomyFlags.CHILD, TaxonomyFlags.STATED);
    public static final EnumSet<TaxonomyFlags> STATED_PARENT_FLAGS_SET = EnumSet.of(TaxonomyFlags.PARENT, TaxonomyFlags.STATED);
    public static EnumSet<TaxonomyFlags> getFlagsFromRelationshipAssertionType(ViewCoordinate viewCoordinate) throws UnsupportedOperationException {
        EnumSet<TaxonomyFlags> flags;
        switch (viewCoordinate.getRelationshipAssertionType()) {
            case INFERRED:
                flags = TaxonomyFlags.INFERRED_PARENT_FLAGS_SET;
                break;
            case STATED:
                flags = TaxonomyFlags.STATED_PARENT_FLAGS_SET;
                break;
            case INFERRED_THEN_STATED:
                flags = TaxonomyFlags.PARENT_FLAG_SET;
                break;
            default:
                throw new UnsupportedOperationException("no support for: " + viewCoordinate.getRelationshipAssertionType());
        }
        return flags;
    }
    
    
    
    
    
    public final int bits;

    TaxonomyFlags(int bits) {
        this.bits = bits;
    }
    
}
