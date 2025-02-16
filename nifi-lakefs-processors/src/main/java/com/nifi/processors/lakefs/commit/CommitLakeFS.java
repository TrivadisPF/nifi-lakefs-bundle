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
import com.nifi.processors.lakefs.branch.CreateBranchLakeFS;
import com.nifi.processors.lakefs.refs.MergeLakeFS;
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.CommitsApi;
import io.lakefs.clients.sdk.model.Commit;
import io.lakefs.clients.sdk.model.CommitCreation;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
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
import java.util.*;

@Tags({"lakefs", "commit", "versioning"})
@CapabilityDescription("""
                        Commit changes to a lakeFS branch. 
                        """)
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@DynamicProperty(name = "The name of a Metadata field to add as metadata to the LakeFS commit operation",
        value = "The value of a Metadata field to add as metadata to the LakeFS commit operation",
        description = "Allows metadata to be added to the LakeFS commit operation as key/value pairs",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@SeeAlso({ WaitCommitLakeFS.class, MergeLakeFS.class, CreateBranchLakeFS.class })
public class CommitLakeFS extends AbstractLakefsProcessor {

    public static final PropertyDescriptor BRANCH_NAME = new PropertyDescriptor
            .Builder()
            .name("branch-name")
            .displayName("Branch Name")
            .description("The name of the branch to commit")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .build();    

    public static final PropertyDescriptor MESSAGE = new PropertyDescriptor.Builder()
            .name("commit-message")
            .displayName("Commit Message")
            .required(false)
            .description("The commit message")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .build();

    public static final PropertyDescriptor METADATA_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("metadata-attribute")
            .displayName("Additional Metadata")
            .required(false)
            .description("Additional metadata to the commit.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .build();

    public static final PropertyDescriptor COMMIT_DATE = new PropertyDescriptor.Builder()
            .name("date")
            .displayName("Date of Commit")
            .description("The commit creation in Unix timestamp seconds format.")
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor FORCE = new PropertyDescriptor.Builder()
            .name("force-commit")
            .displayName("Force Commit?")
            .description("If set to true, then checks that would otherwise block the commit are overwritten, ensuring it proceeds regardless of potential conflicts or inconsistencies." +
                    "If set to false then the commit will fail if there are any conflicts, such as concurrent updates or unresolved changes in the working tree, returning an error.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .defaultValue("false")
            .build(); 

    public static final PropertyDescriptor ALLOW_EMPTY = new PropertyDescriptor.Builder()
            .name("allow-empty")
            .displayName("Allow Empty?")
            .description("If set to true, then a commit be created even when there are no changes (i.e., no modifications, additions, or deletions) in the working tree.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor SOURCE_METARANGE = new PropertyDescriptor.Builder()
            .name("source-metarange")
            .displayName("Source Metarange")
            .required(false)
            .description("This tells lakeFS to use the specified metarange as the base for the new commit")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
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
        Arrays.asList(LAKEFS_SERVICE, REPOSITORY, BRANCH_NAME, MESSAGE, METADATA_ATTRIBUTE, FORCE, ALLOW_EMPTY, SOURCE_METARANGE));
    
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

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        ApiClient apiClient = super.getApiClient(context);

        CommitsApi apiInstance = new CommitsApi(apiClient);
        String repository = context.getProperty(REPOSITORY).evaluateAttributeExpressions(flowFile).getValue();
        String branchName = context.getProperty(BRANCH_NAME).evaluateAttributeExpressions(flowFile).getValue();
        String message = context.getProperty(MESSAGE).evaluateAttributeExpressions(flowFile).getValue();
        long commitDate = Instant.now().getEpochSecond();
        if (context.getProperty(COMMIT_DATE).isSet()) {
            commitDate = context.getProperty(COMMIT_DATE).evaluateAttributeExpressions().asLong();
        }
        boolean force = context.getProperty(FORCE).asBoolean();
        boolean allowEmpty = context.getProperty(ALLOW_EMPTY).asBoolean();
        String sourceMetarange = context.getProperty(SOURCE_METARANGE).evaluateAttributeExpressions(flowFile).getValue();

        // create a metadata map from the dynamic properties added on the Properties tab
        final Map<String, String> metadata = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (entry.getKey().isDynamic() && !StringUtils.isEmpty(entry.getValue())) {
                metadata.put(String.valueOf(entry.getKey()), entry.getValue());
            }
        }

        CommitCreation commitCreation = new CommitCreation(); // CommitCreation | 
        commitCreation.setMessage(message);
        commitCreation.setMetadata(metadata);
        commitCreation.setDate(commitDate);
        commitCreation.setAllowEmpty(allowEmpty);
        commitCreation.setForce(force);
        try {
            Commit result = apiInstance.commit(repository, branchName, commitCreation)
                .sourceMetarange(sourceMetarange)
                .execute();
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling BranchesApi#createBranch");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());

            getLogger().error("Failed to delete branch '{}' in repository {} due to {}", new Object[]{branchName, repository, e.getResponseBody()});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }

        session.transfer(flowFile, REL_SUCCESS);
    }
}
