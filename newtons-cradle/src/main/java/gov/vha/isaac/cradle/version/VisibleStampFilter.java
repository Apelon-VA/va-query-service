/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.version;

import java.util.function.Predicate;

/**
 *
 * @author kec
 */
public class VisibleStampFilter implements Predicate<StampedObject> {

    ViewPoint viewPoint;
    StampSequenceComputer computer;

    public VisibleStampFilter(ViewPoint viewPoint) {
        this.viewPoint = viewPoint;
        this.computer = StampSequenceComputer.getComputer(viewPoint);
    }
    
    
    @Override
    public boolean test(StampedObject t) {
        return this.computer.onRoute(t.getStamp());
    }
    
}
