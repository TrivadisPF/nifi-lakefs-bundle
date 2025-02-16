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
import io.lakefs.clients.sdk.*;
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

import java.util.*;

@Tags({"lakefs", "branch", "versioning"})
@CapabilityDescription("""
                        Reads branch details for the lakefs repository and branch name provided and provide it as flow attributes.
                        """)
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
        @WritesAttribute(attribute = FetchBranchLakeFS.LAKEFS_BRANCH_NAME, description = "The name of the branch"),
        @WritesAttribute(attribute = FetchBranchLakeFS.LAKEFS_BRANCH_COMMIT_ID, description = "The latest commit (id) of the branch"),
})
@SeeAlso({CreateBranchLakeFS.class, DeleteBranchLakeFS.class})
public class FetchBranchLakeFS extends AbstractLakefsProcessor {

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

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are routed to success after being successfully sent to LakeFS")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to failure if unable to be sent to LakeFS")
            .build();            

    public static final List<PropertyDescriptor> descriptors = Collections.unmodifiableList(
        Arrays.asList(LAKEFS_SERVICE, REPOSITORY, BRANCH_NAME));
    
    public static final Set<Relationship> relationships = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    public static final String LAKEFS_BRANCH_NAME = "lakefs.branchId";
    public static final String LAKEFS_BRANCH_COMMIT_ID = "lakefs.branchCommitId";

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

    protected Ref getBranch(ApiClient apiClient, String repository, String branchName) throws ApiException {
        BranchesApi apiInstance = new BranchesApi(apiClient);
        Ref result = apiInstance.getBranch(repository, branchName).execute();
        return result;
    }

    protected Map<String, String> getAttributesFromBranch(final Ref branch) {
        Map<String, String> attributes = new HashMap<>();

        attributes.put(LAKEFS_BRANCH_NAME, branch.getId());
        attributes.put(LAKEFS_BRANCH_COMMIT_ID, branch.getCommitId());
        return attributes;
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
        String branchName = context.getProperty(BRANCH_NAME).evaluateAttributeExpressions(flowFile).getValue();

        try {
            Ref branch = getBranch(apiClient, repository, branchName);
            Map<String, String> attributes = getAttributesFromBranch(branch);
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

            getLogger().error("Failed to get branch '{}' for repositoriy {} due to {}", new Object[]{branchName, repository, e.getResponseBody()});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
