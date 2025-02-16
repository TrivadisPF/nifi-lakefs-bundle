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
package com.nifi.processors.lakefs.refs;

import com.nifi.processors.lakefs.AbstractLakefsProcessor;
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.RefsApi;
import io.lakefs.clients.sdk.model.Merge;
import io.lakefs.clients.sdk.model.MergeResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
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

import java.util.*;

@Tags({"lakefs", "merge", "versioning"})
@CapabilityDescription("""
                        Merge & commit changes from source branch into destination branch
                        """)
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@DynamicProperty(name = "The name of a Metadata field to add as metadata to the LakeFS commit operation",
        value = "The value of a Metadata field to add as metadata to the LakeFS commit operation",
        description = "Allows metadata to be added to the LakeFS commit operation as key/value pairs",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
public class MergeLakeFS extends AbstractLakefsProcessor {

    public static final PropertyDescriptor SOURCE_REF = new PropertyDescriptor
            .Builder()
            .name("source-ref")
            .displayName("Source Reference")
            .description("The name of the source reference")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor DESTINATION_BRANCH = new PropertyDescriptor
            .Builder()
            .name("destination-branch")
            .displayName("Destination Branch Name")
            .description("The name of the destination branch")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MESSAGE = new PropertyDescriptor.Builder()
            .name("merge-message")
            .displayName("Merge Message")
            .required(false)
            .description("The merge message")
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

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are routed to success after being successfully sent to LakeFS")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to failure if unable to be sent to LakeFS")
            .build();            

    public static final List<PropertyDescriptor> descriptors = Collections.unmodifiableList(
        Arrays.asList(LAKEFS_SERVICE, REPOSITORY, SOURCE_REF, DESTINATION_BRANCH, MESSAGE, FORCE, ALLOW_EMPTY));
    
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

        RefsApi apiInstance = new RefsApi(apiClient);
        String repository = context.getProperty(REPOSITORY).evaluateAttributeExpressions(flowFile).getValue();
        String sourceRef = context.getProperty(SOURCE_REF).evaluateAttributeExpressions(flowFile).getValue();
        String destinationBranch = context.getProperty(DESTINATION_BRANCH).evaluateAttributeExpressions(flowFile).getValue();
        String message = context.getProperty(MESSAGE).evaluateAttributeExpressions(flowFile).getValue();
        boolean force = context.getProperty(FORCE).asBoolean();
        boolean allowEmpty = context.getProperty(ALLOW_EMPTY).asBoolean();

        // create a metadata map from the dynamic properties added on the Properties tab
        final Map<String, String> metadata = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (entry.getKey().isDynamic() && !StringUtils.isEmpty(entry.getValue())) {
                metadata.put(String.valueOf(entry.getKey()), entry.getValue());
            }
        }

        Merge merge = new Merge(); // Merge |
        merge.setMessage(message);
        merge.setMetadata(metadata);
        merge.setAllowEmpty(allowEmpty);
        merge.setForce(force);
        try {
            MergeResult result = apiInstance.mergeIntoBranch(repository, sourceRef, destinationBranch)
                .merge(merge)
                .execute();
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling RefsApi#mergeIntoBranch");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());

            getLogger().error("Failed to merge into branch '{}' from source ref {} due to {}", new Object[]{destinationBranch, sourceRef, e.getResponseBody()});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }

        session.transfer(flowFile, REL_SUCCESS);
    }
}
