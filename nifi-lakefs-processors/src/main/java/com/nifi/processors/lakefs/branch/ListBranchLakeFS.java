/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nifi.processors.lakefs.branch;

import com.nifi.processors.lakefs.AbstractLakefsProcessor;
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.BranchesApi;
import io.lakefs.clients.sdk.ObjectsApi;
import io.lakefs.clients.sdk.model.RefList;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.scheduling.SchedulingStrategy;

import java.util.*;

@PrimaryNodeOnly
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"lakefs", "branch", "versioning"})
@CapabilityDescription("""
                        list branches for the lakefs repository provided.
                        """)
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class ListBranchLakeFS extends AbstractLakefsProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are routed to success after being successfully sent to LakeFS")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to failure if unable to be sent to LakeFS")
            .build();            

    public static final List<PropertyDescriptor> descriptors = Collections.unmodifiableList(
        Arrays.asList(LAKEFS_SERVICE, REPOSITORY));
    
    public static final Set<Relationship> relationships = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    protected RefList listBranches(ApiClient apiClient, String repository) throws ApiException {
        BranchesApi apiInstance = new BranchesApi(apiClient);
        RefList result = apiInstance.listBranches(repository).execute();
        return result;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        ApiClient apiClient = super.getApiClient(context);

        ObjectsApi apiInstance = new ObjectsApi(apiClient);
        String repository = context.getProperty(REPOSITORY).evaluateAttributeExpressions(flowFile).getValue();

        try {
            RefList branches = listBranches(apiClient, repository);

            // Transfer to SUCCESS
            session.transfer(flowFile, REL_SUCCESS);
        } catch (ApiException e) {
            System.err.println("Exception when calling ObjectsApi#getObject");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());

            getLogger().error("Failed to list branches for {}", new Object[]{repository, e.getResponseBody()});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
