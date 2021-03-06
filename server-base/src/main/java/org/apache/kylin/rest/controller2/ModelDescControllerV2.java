/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.rest.controller2;

import static org.apache.kylin.metadata.model.DataModelDesc.STATUS_DRAFT;

import java.io.IOException;
import java.util.HashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.DataModelDescResponse;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.ProjectServiceV2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author jiazhong
 * 
 */
@Controller
@RequestMapping(value = "/model")
public class ModelDescControllerV2 extends BasicController {

    @Autowired
    @Qualifier("projectServiceV2")
    private ProjectServiceV2 projectServiceV2;

    /**
     * Get detail information of the "Model ID"
     * 
     * @param modelName
     *            Model ID
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{modelName}", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getModelV2(@RequestHeader("Accept-Language") String lang, @PathVariable String modelName) {
        MsgPicker.setMsg(lang);
        Message msg = MsgPicker.getMsg();

        HashMap<String, DataModelDescResponse> data = new HashMap<String, DataModelDescResponse>();

        MetadataManager metaManager = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        DataModelDesc modelDesc = metaManager.getDataModelDesc(modelName);
        if (modelDesc == null)
            throw new BadRequestException(String.format(msg.getMODEL_NOT_FOUND(), modelName));

        DataModelDescResponse dataModelDescResponse = new DataModelDescResponse(modelDesc);
        dataModelDescResponse.setProject(projectServiceV2.getProjectOfModel(modelName));

        if (modelDesc.getStatus() == null) {
            data.put("model", dataModelDescResponse);

            String draftName = modelName + "_draft";
            DataModelDesc draftDesc = metaManager.getDataModelDesc(draftName);
            if (draftDesc != null && draftDesc.getStatus() != null && draftDesc.getStatus().equals(STATUS_DRAFT)) {
                DataModelDescResponse draftModelDescResponse = new DataModelDescResponse(draftDesc);
                draftModelDescResponse.setProject(projectServiceV2.getProjectOfModel(draftName));
                data.put("draft", draftModelDescResponse);
            }
        } else if (modelDesc.getStatus().equals(STATUS_DRAFT)) {
            data.put("draft", dataModelDescResponse);

            String parentName = modelName.substring(0, modelName.lastIndexOf("_draft"));
            DataModelDesc parentDesc = metaManager.getDataModelDesc(parentName);
            if (parentDesc != null && parentDesc.getStatus() == null) {
                DataModelDescResponse parentModelDescResponse = new DataModelDescResponse(parentDesc);
                parentModelDescResponse.setProject(projectServiceV2.getProjectOfModel(parentName));
                data.put("model", parentModelDescResponse);
            }
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

}
