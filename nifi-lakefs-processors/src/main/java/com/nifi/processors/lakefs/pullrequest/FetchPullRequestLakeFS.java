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
import com.nifi.processors.lakefs.branch.CreateBranchLakeFS;
import com.nifi.processors.lakefs.branch.DeleteBranchLakeFS;
import io.lakefs.clients.sdk.*;
import io.lakefs.clients.sdk.model.PullRequest;
import io.lakefs.clients.sdk.model.Ref;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Tags({"lakefs", "pull-request", "versioning"})
@CapabilityDescription("""
                        Get pull request details and provide as flow attributes for the lakefs repository and pull request id.
                        """)
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
        @WritesAttribute(attribute = FetchPullRequestLakeFS.LAKEFS_PULL_REQUEST_STATUS, description = "The status of the pull request"),
        @WritesAttribute(attribute = FetchPullRequestLakeFS.LAKEFS_PULL_REQUEST_TITLE, description = "The title of the pull request"),
        @WritesAttribute(attribute = FetchPullRequestLakeFS.LAKEFS_PULL_REQUEST_DESCRIPTION, description = "The description of the pull request"),
        @WritesAttribute(attribute = FetchPullRequestLakeFS.LAKEFS_PULL_REQUEST_ID, description = "The id of the pull request"),
        @WritesAttribute(attribute = FetchPullRequestLakeFS.LAKEFS_PULL_REQUEST_CREATION_DATE, description = "The creation date of the pull request"),
        @WritesAttribute(attribute = FetchPullRequestLakeFS.LAKEFS_PULL_REQUEST_AUTHOR, description = "The author of the pull request"),
        @WritesAttribute(attribute = FetchPullRequestLakeFS.LAKEFS_PULL_REQUEST_SOURCE_BRANCH, description = "The source branch of the pull request"),
        @WritesAttribute(attribute = FetchPullRequestLakeFS.LAKEFS_PULL_REQUEST_DESTINATION_BRANCH, description = "The destination branch of the pull request"),
        @WritesAttribute(attribute = FetchPullRequestLakeFS.LAKEFS_PULL_REQUEST_MERGED_COMMIT_ID, description = "The merged commit id of the pull request"),
        @WritesAttribute(attribute = FetchPullRequestLakeFS.LAKEFS_PULL_REQUEST_CLOSED_DATE, description = "The request closed date of the pull request"),
})
@SeeAlso({CreateBranchLakeFS.class, DeleteBranchLakeFS.class})
public class FetchPullRequestLakeFS extends AbstractLakefsProcessor {

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

    public static final String LAKEFS_PULL_REQUEST_STATUS = "lakefs.pullRequestStatus";
    public static final String LAKEFS_PULL_REQUEST_TITLE = "lakefs.pullRequestTitle";
    public static final String LAKEFS_PULL_REQUEST_DESCRIPTION = "lakefs.pullRequestDescription";
    public static final String LAKEFS_PULL_REQUEST_ID = "lakefs.pullRequestId";
    public static final String LAKEFS_PULL_REQUEST_CREATION_DATE = "lakefs.pullRequestCreationDate";
    public static final String LAKEFS_PULL_REQUEST_AUTHOR = "lakefs.pullRequestAuthor";
    public static final String LAKEFS_PULL_REQUEST_SOURCE_BRANCH = "lakefs.pullRequestSourceBranch";
    public static final String LAKEFS_PULL_REQUEST_DESTINATION_BRANCH = "lakefs.pullRequestDestinationBranch";
    public static final String LAKEFS_PULL_REQUEST_MERGED_COMMIT_ID = "lakefs.pullRequestMergedCommitId";
    public static final String LAKEFS_PULL_REQUEST_CLOSED_DATE = "lakefs.pullRequestClosedDate";

    public static final String DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(DATE_ATTR_FORMAT, Locale.US);

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

    protected PullRequest getPullRequest(ApiClient apiClient, String repository, String pullRequestId) throws ApiException {
        ExperimentalApi apiInstance = new ExperimentalApi(apiClient);
        PullRequest result = apiInstance.getPullRequest(repository, pullRequestId).execute();
        return result;
    }

    private String formatDateTime(OffsetDateTime dateTime) {
        return DATE_TIME_FORMATTER.format(dateTime);
    }

    protected Map<String, String> getAttributesFromPullRequest(final PullRequest pullRequest) {
        Map<String, String> attributes = new HashMap<>();

        attributes.put(LAKEFS_PULL_REQUEST_STATUS, pullRequest.getStatus().getValue());
        attributes.put(LAKEFS_PULL_REQUEST_TITLE, pullRequest.getTitle());
        attributes.put(LAKEFS_PULL_REQUEST_DESCRIPTION, pullRequest.getDescription());
        attributes.put(LAKEFS_PULL_REQUEST_ID, pullRequest.getId());
        attributes.put(LAKEFS_PULL_REQUEST_CREATION_DATE, formatDateTime(pullRequest.getCreationDate()));
        attributes.put(LAKEFS_PULL_REQUEST_AUTHOR, pullRequest.getAuthor());
        attributes.put(LAKEFS_PULL_REQUEST_SOURCE_BRANCH, pullRequest.getSourceBranch());
        attributes.put(LAKEFS_PULL_REQUEST_DESTINATION_BRANCH, pullRequest.getDestinationBranch());
        attributes.put(LAKEFS_PULL_REQUEST_MERGED_COMMIT_ID, pullRequest.getMergedCommitId());
        attributes.put(LAKEFS_PULL_REQUEST_CLOSED_DATE, formatDateTime(pullRequest.getClosedDate()));

        return attributes;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        ApiClient apiClient = super.getApiClient(context);

        String repository = context.getProperty(REPOSITORY).evaluateAttributeExpressions(flowFile).getValue();
        String pullRequestId = context.getProperty(PULL_REQUEST_ID).evaluateAttributeExpressions(flowFile).getValue();

        try {
            PullRequest pullRequest = getPullRequest(apiClient, repository, pullRequestId);
            Map<String, String> attributes = getAttributesFromPullRequest(pullRequest);
            if (!attributes.isEmpty()) {
                flowFile = session.putAllAttributes(flowFile, attributes);
            }

            // Transfer to SUCCESS
            session.transfer(flowFile, REL_SUCCESS);
        } catch (ApiException e) {
            System.err.println("Exception when calling ObjectsApi#getObject");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());

            getLogger().error("Failed to get pull request '{}' for repository {} due to {}", new Object[]{pullRequestId, repository, e.getResponseBody()});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
