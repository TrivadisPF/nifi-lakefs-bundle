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
                        Create a new pull request in a repository in LakeFS. 
                        """)
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
public class CreatePullRequestLakeFS extends AbstractLakefsProcessor {

    public static final PropertyDescriptor TITLE = new PropertyDescriptor
            .Builder()
            .name("title")
            .displayName("Title")
            .description("The title for the pull request")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor DESCRIPTION = new PropertyDescriptor
            .Builder()
            .name("description")
            .displayName("Description")
            .description("The description for the pull request")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor SOURCE_BRANCH = new PropertyDescriptor
            .Builder()
            .name("source-branch")
            .displayName("Source Branch Name")
            .description("The name of the source branch for the pull-request")
            .required(true)
            .defaultValue("main")            
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor TARGET_BRANCH = new PropertyDescriptor
            .Builder()
            .name("target-branch")
            .displayName("Target Branch Name")
            .description("The name of the target branch for pull-request")
            .required(true)
            .defaultValue("main")
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
        Arrays.asList(LAKEFS_SERVICE, REPOSITORY, TITLE, DESCRIPTION, SOURCE_BRANCH, TARGET_BRANCH));
    
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

    protected PullRequestCreationResponse createPullRequest(ApiClient apiClient, String repository, String title, String description, String sourceBranch, String targetBranch) throws ApiException {
        PullRequestCreationResponse result = null;
        ExperimentalApi apiInstance = new ExperimentalApi(apiClient);
        try {
            PullRequestCreation pullRequestCreation = new PullRequestCreation();
            pullRequestCreation.setTitle(title);
            pullRequestCreation.setDescription(description);
            pullRequestCreation.setSourceBranch(sourceBranch);
            pullRequestCreation.setDestinationBranch(targetBranch);

            result = apiInstance.createPullRequest(repository, pullRequestCreation).execute();
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
        String title = context.getProperty(TITLE).evaluateAttributeExpressions(finalFlowFile).getValue();
        String description = context.getProperty(DESCRIPTION).evaluateAttributeExpressions(finalFlowFile).getValue();
        String sourceBranch = context.getProperty(SOURCE_BRANCH).evaluateAttributeExpressions(finalFlowFile).getValue();
        String targetBranch = context.getProperty(TARGET_BRANCH).evaluateAttributeExpressions(finalFlowFile).getValue();

        try {
            createPullRequest(apiClient, repository, title, description, sourceBranch, targetBranch);
        } catch (ApiException e) {
            System.err.println("Exception when calling ExperimentalApi#createPullRequest");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());

            getLogger().error("Failed to create pull request from branch {} to branch {} due to {}", new Object[]{sourceBranch, targetBranch, e.getResponseBody()});
            finalFlowFile = session.penalize(finalFlowFile);
            session.transfer(finalFlowFile, REL_FAILURE);
        }

        session.transfer(finalFlowFile, REL_SUCCESS);
    }
}
