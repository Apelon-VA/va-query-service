package org.ihtsdo.otf.query.integration.tests;

/*
 * Copyright 2013 International Health Terminology Standards Development Organisation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import gov.vha.isaac.metadata.coordinates.ViewCoordinates;
import java.io.IOException;
import java.util.EnumSet;
import java.util.GregorianCalendar;
import org.ihtsdo.otf.tcc.api.coordinate.Position;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;

/**
 * Class to adjust the
 * <code>ViewCoordinate</code>, which can be used in difference queries.
 *
 * @author dylangrald
 */
public final class SetViewCoordinate {

    ViewCoordinate v1;
    Position position;

    public SetViewCoordinate(int year, int month, int day, int hour, int minute) throws IOException {
        this.position = new Position();
        this.v1 = ViewCoordinates.getDevelopmentInferredLatestActiveOnly();
        position = v1.getViewPosition();
        setPositionTime(year, month, day, hour, minute);
        v1.setViewPosition(position);
        v1.setAllowedStatus(EnumSet.of(Status.ACTIVE));
    }

    /**
     * Sets the
     * <code>Position</code> time with the given date and time set for the
     * default time zone with the default locale.
     *
     * @param year the value used to set the <code>YEAR</code> calendar field in
     * the calendar.
     * @param month the value used to set the <code>MONTH</code> calendar field
     * in the calendar. Month value is 0-based. e.g., 0 for January.
     * @param day the value used to set the <code>DAY_OF_MONTH</code> calendar
     * field in the calendar.
     * @param hour the value used to set the <code>HOUR_OF_DAY</code> calendar
     * field in the calendar.
     * @param minute the value used to set the <code>MINUTE</code> calendar
     * field in the calendar.
     */
    public void setPositionTime(int year, int month, int day, int hour, int minute) {
        GregorianCalendar calendar = new GregorianCalendar(year, month, day, hour, minute);
        long time = calendar.getTimeInMillis();
        this.position.setTime(time);
    }

    public ViewCoordinate getViewCoordinate() {
        return this.v1;
    }
}
