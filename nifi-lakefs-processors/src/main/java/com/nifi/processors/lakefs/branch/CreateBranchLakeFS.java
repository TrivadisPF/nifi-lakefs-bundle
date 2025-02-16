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
import io.lakefs.clients.sdk.model.BranchCreation;
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

@Tags({"lakefs", "branch", "versioning"})
@CapabilityDescription("""
                        Create a new branch in a repository in LakeFS. It allows to create a 
                        new branch based on an existing branch or commit, providing an isolated workspace 
                        for experimentation, testing, or development without affecting the source. 
                        """)
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
public class CreateBranchLakeFS extends AbstractLakefsProcessor {

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

    public static final PropertyDescriptor SOURCE = new PropertyDescriptor
            .Builder()
            .name("source")
            .displayName("Source Branch Name")
            .description("The name of the source branch to create the new branch from")
            .required(true)
            .defaultValue("main")            
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .build();    

    public static final PropertyDescriptor FORCE = new PropertyDescriptor.Builder()
            .name("force-create")
            .displayName("Overwrite if existing?")
            .description("If set to true, and a branch with the specified name already exists, it will be overwritten." +
                    "If set to false and a branch with the specified name already exists, the operation will fail, returning an error")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor HIDDEN = new PropertyDescriptor.Builder()
            .name("hidden")
            .displayName("Hide branch?")
            .description("If set to true, branch will not show up when listing branches by default")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor IGNORE_ALREADY_EXITS = new PropertyDescriptor.Builder()
            .name("ignore-exists")
            .displayName("Ignore Branch exits exception?")
            .description("If set to true, an error raised when the branch already exists is ignored!")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .defaultValue("true")
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
        Arrays.asList(LAKEFS_SERVICE, REPOSITORY, BRANCH_NAME, SOURCE, FORCE, HIDDEN, IGNORE_ALREADY_EXITS));
    
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

    protected String createBranch(ApiClient apiClient,String repository, String branchName, String source, Boolean force, Boolean hidden, boolean ignoreAlreadyExitsException) throws ApiException {
        String result = null;
        BranchesApi apiInstance = new BranchesApi(apiClient);
        try {
            BranchCreation branchCreation = new BranchCreation();
            branchCreation.setSource(source);
            branchCreation.setName(branchName);
            if (force != null) {
                branchCreation.setForce(force);
            }
            if (hidden != null) {
                branchCreation.setHidden(hidden);
            }

            result = apiInstance.createBranch(repository, branchCreation).execute();
        } catch (ApiException ex) {
            if (ignoreAlreadyExitsException && ex.getResponseBody().contains("branch already exists")) {
                getLogger().warn("Branch " + branchName + " already exists, ignoring it!");
            } else {
                throw ex;
            }
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

        BranchesApi apiInstance = new BranchesApi(apiClient);
        String repository = context.getProperty(REPOSITORY).evaluateAttributeExpressions(finalFlowFile).getValue();
        String branchName = context.getProperty(BRANCH_NAME).evaluateAttributeExpressions(finalFlowFile).getValue();
        String source = context.getProperty(SOURCE).evaluateAttributeExpressions(finalFlowFile).getValue();
        Boolean force = context.getProperty(FORCE).asBoolean();
        Boolean hidden = context.getProperty(HIDDEN).asBoolean();
        Boolean ignoreAlreadyExistsException = context.getProperty(IGNORE_ALREADY_EXITS).asBoolean();

        try {
            createBranch(apiClient, repository, branchName, source, force, hidden, ignoreAlreadyExistsException);
        } catch (ApiException e) {
            System.err.println("Exception when calling BranchesApi#createBranch");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());

            getLogger().error("Failed to create branch '{}' in repository {} due to {}", new Object[]{branchName, repository, e.getResponseBody()});
            finalFlowFile = session.penalize(finalFlowFile);
            session.transfer(finalFlowFile, REL_FAILURE);
        }

        session.transfer(finalFlowFile, REL_SUCCESS);
    }
}
