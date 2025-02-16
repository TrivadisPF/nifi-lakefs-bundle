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
package com.nifi.processors.lakefs.commit;

import com.nifi.processors.lakefs.AbstractLakefsProcessor;
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.BranchesApi;
import io.lakefs.clients.sdk.CommitsApi;
import io.lakefs.clients.sdk.model.Commit;
import io.lakefs.clients.sdk.model.Ref;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
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

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.nifi.processors.lakefs.commit.WaitCommitLakeFS.*;

@TriggerWhenEmpty
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({"lakefs", "commit", "versioning"})
@CapabilityDescription("""
                       Waits on a branch until that branch was committed.
                        """)
@WritesAttributes({
        @WritesAttribute(attribute = LAKEFS_COMMIT_ID, description = "The id of the commit"),
        @WritesAttribute(attribute = LAKEFS_COMMITER, description = "The username of the commiter"),
        @WritesAttribute(attribute = LAKEFS_COMMIT_MESSAGE, description = "The commit message"),
        @WritesAttribute(attribute = LAKEFS_COMMIT_METADATA, description = "The metadata of the commit, a map represented as a string in the form {key1=val1, key2=val2, ...}"),
        @WritesAttribute(attribute = LAKEFS_COMMIT_METARANGE_ID, description = "The metarange id"),
        @WritesAttribute(attribute = LAKEFS_COMMIT_CREATION_DATE, description = "The creation date"),
        @WritesAttribute(attribute = LAKEFS_COMMIT_VERSION, description = "The version"),
        @WritesAttribute(attribute = LAKEFS_COMMIT_GENERATION, description = "The generation"),
        @WritesAttribute(attribute = LAKEFS_COMMIT_PARENTS, description = "The parent(s) commit id, a list represented as a string in the form of [parent1, parent2, ...]")
})
@SeeAlso({FetchCommitLakeFS.class, CommitLakeFS.class})
public class WaitCommitLakeFS extends AbstractLakefsProcessor {

    public static final PropertyDescriptor BRANCH_NAME = new PropertyDescriptor
            .Builder()
            .name("branch-name")
            .displayName("Branch Name")
            .description("The name of the branch")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor PREVIOUS_COMMIT_ID = new PropertyDescriptor.Builder()
            .name("Previous Commit Id")
            .displayName("Previous Commit Id")
            .description("the id of the last commit")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor POLLING_INTERVAL = new PropertyDescriptor.Builder()
            .name("Polling Interval")
            .description("Indicates how long to wait before performing a directory listing")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 sec")
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
        Arrays.asList(LAKEFS_SERVICE, REPOSITORY, BRANCH_NAME, PREVIOUS_COMMIT_ID, POLLING_INTERVAL));
    
    public static final Set<Relationship> relationships = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    private final Lock checkLock = new ReentrantLock();
    private final AtomicLong lastChecked = new AtomicLong(0L);
    private final AtomicReference<String> lastCommitId = new AtomicReference<>(null);

    public static final String LAKEFS_COMMIT_ID = "lakefs.commitId";
    public static final String LAKEFS_COMMITER = "lakefs.committer";
    public static final String LAKEFS_COMMIT_MESSAGE = "lakefs.commitMessage";
    public static final String LAKEFS_COMMIT_METADATA = "lakefs.commitMetadata";
    public static final String LAKEFS_COMMIT_METARANGE_ID = "lakefs.commitMetaRangeId";
    public static final String LAKEFS_COMMIT_CREATION_DATE = "lakefs.commitCreationDate";
    public static final String LAKEFS_COMMIT_VERSION = "lakefs.commitVersion";
    public static final String LAKEFS_COMMIT_GENERATION = "lakefs.commitGeneration";
    public static final String LAKEFS_COMMIT_PARENTS = "lakefs.commitParents";

    public static final String COMMIT_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(COMMIT_DATE_ATTR_FORMAT, Locale.US);

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

    protected String getCommitId(ApiClient apiClient, String repository, String branchName) throws ApiException {
        String commitId = null;
        BranchesApi apiInstance = new BranchesApi(apiClient);
        try {
            Ref result = apiInstance.getBranch(repository, branchName).execute();
            commitId = result.getCommitId();
        } catch (ApiException ex) {
            if (ex.getResponseBody().contains("branch not found")) {
                getLogger().warn("Branch " + branchName + " does not exist!");
            } else {
                throw ex;
            }
        }
        return commitId;
    }

    public Commit getCommit(ApiClient apiClient, String repository, String commitId) throws ApiException {
        CommitsApi apiInstance = new CommitsApi(apiClient);
        Commit result = apiInstance.getCommit(repository, commitId).execute();
        return result;
    }

    protected Map<String, String> getAttributesFromCommit(final Commit commit) {
        Map<String, String> attributes = new HashMap<>();

        attributes.put(LAKEFS_COMMIT_ID, commit.getId());
        attributes.put(LAKEFS_COMMITER, commit.getCommitter());
        attributes.put(LAKEFS_COMMIT_MESSAGE, commit.getMessage());
        attributes.put(LAKEFS_COMMIT_METARANGE_ID, commit.getMetaRangeId());
        attributes.put(LAKEFS_COMMIT_CREATION_DATE, formatDateTime(commit.getCreationDate()));
        attributes.put(LAKEFS_COMMIT_VERSION, String.valueOf(commit.getVersion()));
        attributes.put(LAKEFS_COMMIT_GENERATION, String.valueOf(commit.getGeneration()));
        attributes.put(LAKEFS_COMMIT_PARENTS, String.valueOf(commit.getParents().toString()));
        return attributes;
    }

    private String formatDateTime(final long dateTime) {
        final ZonedDateTime zonedDateTime = Instant.ofEpochMilli(dateTime).atZone(ZoneId.systemDefault());
        return DATE_TIME_FORMATTER.format(zonedDateTime);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile input = null;

        if (context.hasIncomingConnection()) {
            input = session.get();
            if (input == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        ApiClient apiClient = super.getApiClient(context);
        String repository = context.getProperty(REPOSITORY).getValue();
        String branchName = context.getProperty(BRANCH_NAME).getValue();
        if (context.getProperty(PREVIOUS_COMMIT_ID).isSet()) {
            lastCommitId.set(context.getProperty(PREVIOUS_COMMIT_ID).evaluateAttributeExpressions(input).getValue());
        }

        // We need a FlowFile to report provenance correctly.
        FlowFile output = null;
        try {
            final long pollingMillis = context.getProperty(POLLING_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);
            if ((lastChecked.get() < System.currentTimeMillis() - pollingMillis) && checkLock.tryLock()) {
                try {
                    //if (lastCommitId.get() == null) {
                    //    lastCommitId.set(getCommitId(apiClient, repository, branchName));
                    //    context.yield();
                    //    return;
                    //}

                    String currentCommitId = getCommitId(apiClient, repository, branchName);
                    if (currentCommitId == null) {
                        lastCommitId.set(null);
                    }
                    getLogger().info("Last commitID = " + lastCommitId.get());
                    getLogger().info("Current commitID = " + currentCommitId);

                    lastChecked.set(System.currentTimeMillis());

                    if ((lastCommitId.get() == null && currentCommitId != null) ||
                        currentCommitId != null && !lastCommitId.get().equals(currentCommitId)) {
                        output = input != null ? input : session.create();
                        lastCommitId.set(currentCommitId);

                        Map<String, String> attributes = getAttributesFromCommit(getCommit(apiClient, repository, currentCommitId));
                        if (!attributes.isEmpty()) {
                            output = session.putAllAttributes(output, attributes);
                        }

                        session.transfer(output, REL_SUCCESS);
                    } else {
                        context.yield();
                    }
                } finally {
                    checkLock.unlock();
                }
            }

        } catch (ApiException e) {
            System.err.println("Exception when calling CommitsApi#getBranch");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());

            getLogger().error("Failed to get branch for branch '{}' in repository {} due to {}", new Object[]{branchName, repository, e.getResponseBody()});

            if (input != null) {
                session.transfer(input, REL_FAILURE);
            }
        }
    }

}
