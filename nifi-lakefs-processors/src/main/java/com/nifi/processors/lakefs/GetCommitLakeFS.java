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
package com.nifi.processors.lakefs;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.eclipse.jetty.util.security.Password;

import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.*;
import io.lakefs.clients.sdk.model.Commit;
import io.lakefs.clients.sdk.model.CommitCreation;
import io.lakefs.clients.sdk.ActionsApi;
import io.lakefs.clients.sdk.BranchesApi;
import io.lakefs.clients.sdk.CommitsApi;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"lakefs", "versioning"})
@CapabilityDescription("""
                       Get commit details for a lakeFS ref.
                        """)
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class GetCommitLakeFS extends AbstractLakefsProcessor {

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
            .required(true)
            .description("The commit message")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .build();

    public static final PropertyDescriptor METADATA_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("metadata-attribute")
            .displayName("Additional Metadata")
            .required(true)
            .description("Additional metadata to the commit.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
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
        Arrays.asList(LAKEFS_URL, USERNAME, PASSWORD, REPOSITORY, BRANCH_NAME, MESSAGE, METADATA_ATTRIBUTE, FORCE, ALLOW_EMPTY, SOURCE_METARANGE));
    
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

        ApiClient apiClient = super.createClient(context);

        CommitsApi apiInstance = new CommitsApi(apiClient);
        String repository = context.getProperty(REPOSITORY).evaluateAttributeExpressions(flowFile).getValue();
        String branchName = context.getProperty(BRANCH_NAME).evaluateAttributeExpressions(flowFile).getValue();
        String message = context.getProperty(MESSAGE).evaluateAttributeExpressions(flowFile).getValue();
        boolean force = context.getProperty(FORCE).asBoolean();
        boolean allowEmpty = context.getProperty(ALLOW_EMPTY).asBoolean();
        String sourceMetarange = context.getProperty(SOURCE_METARANGE).evaluateAttributeExpressions(flowFile).getValue();

        CommitCreation commitCreation = new CommitCreation(); // CommitCreation | 
        commitCreation.setMessage(message);
        commitCreation.setMetadata(null);
        commitCreation.setDate(Instant.now().getEpochSecond());
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
