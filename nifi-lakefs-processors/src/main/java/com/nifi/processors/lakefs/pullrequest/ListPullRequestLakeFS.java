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
package com.nifi.processors.lakefs.pullrequest;

import com.nifi.processors.lakefs.AbstractLakefsProcessor;
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.ExperimentalApi;
import io.lakefs.clients.sdk.ObjectsApi;
import io.lakefs.clients.sdk.model.PullRequestsList;
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
@Tags({"lakefs", "pull-request", "versioning"})
@CapabilityDescription("""
                        List pull requests for the lakefs repository provided.
                        """)
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class ListPullRequestLakeFS extends AbstractLakefsProcessor {

    public static final PropertyDescriptor PREFIX = new PropertyDescriptor
            .Builder()
            .name("prefix")
            .displayName("Prefixed with")
            .description("Return items prefixed with this value")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor AFTER = new PropertyDescriptor
            .Builder()
            .name("after")
            .displayName("After this value")
            .description("Return items after this value")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor AMOUNT = new PropertyDescriptor
            .Builder()
            .name("amount")
            .displayName("Number Items to return")
            .description("How many items to return")
            .required(false)
            .defaultValue("1")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor STATUS = new PropertyDescriptor.Builder()
            .name("status")
            .displayName("For status")
            .description("Return items for status")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("open", "closed", "all")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .defaultValue("all")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are routed to success after being successfully sent to LakeFS")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to failure if unable to be sent to LakeFS")
            .build();            

    public static final List<PropertyDescriptor> descriptors = Collections.unmodifiableList(
        Arrays.asList(LAKEFS_SERVICE, REPOSITORY, PREFIX, AFTER, AMOUNT, STATUS));
    
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

    protected PullRequestsList listPullRequests(ApiClient apiClient, String repository, String prefix, String after, Integer amount, String status) throws ApiException {
        ExperimentalApi apiInstance = new ExperimentalApi(apiClient);
        PullRequestsList result = apiInstance.listPullRequests(repository)
                .prefix(prefix)
                .after(after)
                .amount(amount)
                .status(status)
                .execute();
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
        String prefix = context.getProperty(PREFIX).evaluateAttributeExpressions(flowFile).getValue();
        String after = context.getProperty(AFTER).evaluateAttributeExpressions(flowFile).getValue();
        Integer amount = context.getProperty(AFTER).evaluateAttributeExpressions(flowFile).asInteger();
        String status = context.getProperty(STATUS).evaluateAttributeExpressions(flowFile).getValue();

        try {
            PullRequestsList result = listPullRequests(apiClient, repository, prefix, after, amount, status);

            // Transfer to SUCCESS
            session.transfer(flowFile, REL_SUCCESS);
        } catch (ApiException e) {
            System.err.println("Exception when calling ExperimentalApi#listPullRequests");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());

            getLogger().error("Failed to list pull requests for {} due to {}", new Object[]{repository, e.getResponseBody()});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
