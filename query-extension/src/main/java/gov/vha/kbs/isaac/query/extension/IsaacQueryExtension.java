/*
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
package gov.vha.kbs.isaac.query.extension;

import org.apache.maven.AbstractMavenLifecycleParticipant;
import org.codehaus.plexus.component.annotations.Component;
import org.ihtsdo.otf.tcc.build.extension.DatabaseBuildExtension;


@Component(role = AbstractMavenLifecycleParticipant.class, hint = "isaac-query")
public class IsaacQueryExtension extends DatabaseBuildExtension {

    public IsaacQueryExtension() {
        super("Isaac query");
    }
}
