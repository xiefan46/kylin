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

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.model.Query;
import org.apache.kylin.rest.model.SelectedColumnMeta;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.MetaRequest;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.request.SaveSqlRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.QueryServiceV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.supercsv.prefs.CsvPreference;

/**
 * Handle query requests.
 * 
 * @author xduo
 */
@Controller
public class QueryControllerV2 extends BasicController {

    private static final Logger logger = LoggerFactory.getLogger(QueryControllerV2.class);

    @Autowired
    @Qualifier("queryServiceV2")
    private QueryServiceV2 queryServiceV2;

    @RequestMapping(value = "/query", method = RequestMethod.POST, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse queryV2(@RequestHeader("Accept-Language") String lang, @RequestBody SQLRequest sqlRequest) {
        MsgPicker.setMsg(lang);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, queryServiceV2.doQueryWithCache(sqlRequest), "");
    }

    // TODO should be just "prepare" a statement, get back expected ResultSetMetaData

    @RequestMapping(value = "/query/prestate", method = RequestMethod.POST, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse prepareQueryV2(@RequestHeader("Accept-Language") String lang, @RequestBody PrepareSqlRequest sqlRequest) {
        MsgPicker.setMsg(lang);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, queryServiceV2.doQueryWithCache(sqlRequest), "");
    }

    @RequestMapping(value = "/saved_queries", method = RequestMethod.POST, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void saveQueryV2(@RequestHeader("Accept-Language") String lang, @RequestBody SaveSqlRequest sqlRequest) throws IOException {
        MsgPicker.setMsg(lang);

        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        Query newQuery = new Query(sqlRequest.getName(), sqlRequest.getProject(), sqlRequest.getSql(), sqlRequest.getDescription());

        queryServiceV2.saveQuery(creator, newQuery);
    }

    @RequestMapping(value = "/saved_queries/{id}", method = RequestMethod.DELETE, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void removeQueryV2(@RequestHeader("Accept-Language") String lang, @PathVariable String id) throws IOException {
        MsgPicker.setMsg(lang);

        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        queryServiceV2.removeQuery(creator, id);
    }

    @RequestMapping(value = "/saved_queries/{project}", method = RequestMethod.GET, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getQueriesV2(@RequestHeader("Accept-Language") String lang, @PathVariable String project, @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset, @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize) throws IOException {
        MsgPicker.setMsg(lang);

        HashMap<String, Object> data = new HashMap<String, Object>();
        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        List<Query> queries = new ArrayList<Query>();
        for (Query query : queryServiceV2.getQueries(creator)) {
            if (query.getProject().equals(project))
                queries.add(query);
        }

        int offset = pageOffset * pageSize;
        int limit = pageSize;

        if (queries.size() <= offset) {
            offset = queries.size();
            limit = 0;
        }

        if ((queries.size() - offset) < limit) {
            limit = queries.size() - offset;
        }

        data.put("queries", queries.subList(offset, offset + limit));
        data.put("size", queries.size());

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/query/format/{format}", method = RequestMethod.GET, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void downloadQueryResultV2(@RequestHeader("Accept-Language") String lang, @PathVariable String format, SQLRequest sqlRequest, HttpServletResponse response) {
        MsgPicker.setMsg(lang);

        SQLResponse result = queryServiceV2.doQueryWithCache(sqlRequest);
        response.setContentType("text/" + format + ";charset=utf-8");
        response.setHeader("Content-Disposition", "attachment; filename=\"result." + format + "\"");
        ICsvListWriter csvWriter = null;

        try {
            csvWriter = new CsvListWriter(response.getWriter(), CsvPreference.STANDARD_PREFERENCE);

            List<String> headerList = new ArrayList<String>();

            for (SelectedColumnMeta column : result.getColumnMetas()) {
                headerList.add(column.getName());
            }

            String[] headers = new String[headerList.size()];
            csvWriter.writeHeader(headerList.toArray(headers));

            for (List<String> row : result.getResults()) {
                csvWriter.write(row);
            }
        } catch (IOException e) {
            throw new InternalErrorException(e);
        } finally {
            IOUtils.closeQuietly(csvWriter);
        }
    }

    @RequestMapping(value = "/tables_and_columns", method = RequestMethod.GET, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getMetadataV2(@RequestHeader("Accept-Language") String lang, MetaRequest metaRequest) throws SQLException, IOException {
        MsgPicker.setMsg(lang);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, queryServiceV2.getMetadataV2(metaRequest.getProject()), "");
    }

    public void setQueryService(QueryServiceV2 queryService) {
        this.queryServiceV2 = queryService;
    }
}
