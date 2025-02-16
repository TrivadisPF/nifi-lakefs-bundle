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
import io.lakefs.clients.sdk.model.MergeResult;
import io.lakefs.clients.sdk.model.PullRequestCreation;
import io.lakefs.clients.sdk.model.PullRequestCreationResponse;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

@Tags({"lakefs", "pull-request", "versioning"})
@CapabilityDescription("""
                        Merge a pull request in a repository of LakeFS. 
                        """)
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
public class MergePullRequestLakeFS extends AbstractLakefsProcessor {

    public static final PropertyDescriptor PULL_REQUEST_ID = new PropertyDescriptor
            .Builder()
            .name("pull-request-id")
            .displayName("Pull Request ID")
            .description("The id of the pull request to merge")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
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
        Arrays.asList(LAKEFS_SERVICE, REPOSITORY, PULL_REQUEST_ID));
    
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

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    protected MergeResult mergePullRequest(ApiClient apiClient, String repository, String pullRequestId) throws ApiException {
        MergeResult result = null;
        ExperimentalApi apiInstance = new ExperimentalApi(apiClient);
        try {
            result = apiInstance.mergePullRequest(repository, pullRequestId).execute();
        } catch (ApiException e) {
            throw e;
        }
        return result;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile originalFlowFile = session.get();

        // If this processor has an incoming connection, then do not run unless a
        // FlowFile is actually sent through
        if (originalFlowFile == null && context.hasIncomingConnection()) {
            return;
        }

        // We need a FlowFile to report provenance correctly.
        FlowFile finalFlowFile = originalFlowFile != null ? originalFlowFile : session.create();

        ApiClient apiClient = super.getApiClient(context);

        String repository = context.getProperty(REPOSITORY).evaluateAttributeExpressions(finalFlowFile).getValue();
        String pullRequestId = context.getProperty(PULL_REQUEST_ID).evaluateAttributeExpressions(finalFlowFile).getValue();

        try {
            mergePullRequest(apiClient, repository, pullRequestId);
        } catch (ApiException e) {
            System.err.println("Exception when calling ExperimentalApi#mergePullRequest");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());

            getLogger().error("Failed to merge pull request for pull request id {} due to {}", new Object[]{pullRequestId, e.getResponseBody()});
            finalFlowFile = session.penalize(finalFlowFile);
            session.transfer(finalFlowFile, REL_FAILURE);
        }

        session.transfer(finalFlowFile, REL_SUCCESS);
    }
}
