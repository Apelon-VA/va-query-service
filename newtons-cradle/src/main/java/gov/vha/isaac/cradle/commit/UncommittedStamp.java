/*
 * Copyright 2015 kec.
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
package gov.vha.isaac.cradle.commit;

import gov.vha.isaac.ochre.api.Get;
import gov.vha.isaac.ochre.api.State;
import org.ihtsdo.otf.tcc.api.hash.Hashcode;
import org.ihtsdo.otf.tcc.api.store.Ts;

/**
 *
 * @author kec
 */
public class UncommittedStamp {
   public int hashCode = Integer.MAX_VALUE;
   public int authorSequence;
   public int pathSequence;
   public State status;
   public int moduleSequence;

   //~--- constructors --------------------------------------------------------

   public UncommittedStamp(State status, int authorSequence, int moduleSequence, int pathSequence) {
      super();
      this.status = status;
      this.authorSequence = Get.identifierService().getConceptSequence(authorSequence);
      this.pathSequence   = Get.identifierService().getConceptSequence(pathSequence);
      this.moduleSequence = Get.identifierService().getConceptSequence(moduleSequence);
   }

   //~--- methods -------------------------------------------------------------

   @Override
   public boolean equals(Object obj) {
      if (obj instanceof UncommittedStamp) {
         UncommittedStamp other = (UncommittedStamp) obj;

         if ((status == other.status) && (authorSequence == other.authorSequence) 
                 && (pathSequence == other.pathSequence) && (moduleSequence == other.moduleSequence)) {
            return true;
         }
      }

      return false;
   }

   @Override
   public int hashCode() {
      if (hashCode == Integer.MAX_VALUE) {
         hashCode = Hashcode.compute(new int[] { status.ordinal(), authorSequence, pathSequence, moduleSequence });
      }

      return hashCode;
   }
    @Override
    public String toString() {
        if (Ts.get() != null) {
            StringBuilder sb = new StringBuilder();
            sb.append("UncommittedStamp{s:");
             sb.append(status);
             sb.append(", a:");
             sb.append(Ts.get().informAboutNid(Get.identifierService().getConceptNid(authorSequence)));
             sb.append(", m:");
             sb.append(Ts.get().informAboutNid(Get.identifierService().getConceptNid(moduleSequence)));
             sb.append(", p: ");
             sb.append(Ts.get().informAboutNid(Get.identifierService().getConceptNid(pathSequence)));
             sb.append('}');
             return sb.toString();
        }
        
        return "UncommittedStamp{s:" + status + ", a:" + authorSequence + 
                ", m:" + moduleSequence + ", p: " + pathSequence +'}';
    }
    
}
