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
package com.nifi.processors.lakefs.objects;

import com.nifi.processors.lakefs.AbstractLakefsProcessor;
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.ObjectsApi;
import io.lakefs.clients.sdk.model.ObjectStats;
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

@Tags({"lakefs", "versioning"})
@CapabilityDescription("""
                        Upload an object to a lakeFS repo.
                        """)
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class PutObjectLakeFS extends AbstractLakefsProcessor {

    public static final PropertyDescriptor BRANCH = new PropertyDescriptor
            .Builder()
            .name("branch")
            .displayName("Branch")
            .description("The branch name to upload the object to.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor PATH = new PropertyDescriptor
            .Builder()
            .name("path")
            .displayName("Path")
            .description("The path to upload to")
            .required(true)
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
        Arrays.asList(LAKEFS_SERVICE, REPOSITORY, BRANCH, PATH, FORCE));
    
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

        ObjectsApi apiInstance = new ObjectsApi(apiClient);
        String repository = context.getProperty(REPOSITORY).evaluateAttributeExpressions(flowFile).getValue();
        String branch = context.getProperty(BRANCH).evaluateAttributeExpressions(flowFile).getValue();
        String path = context.getProperty(PATH).evaluateAttributeExpressions(flowFile).getValue();
        boolean force = context.getProperty(FORCE).asBoolean();

        File content = null;

        try {
            // Create a temporary file
            content = File.createTempFile("nifi-flowfile", ".tmp");

            // Write FlowFile content to the temporary file
            try (FileOutputStream fos = new FileOutputStream(content)) {
                session.read(flowFile, rawIn -> {
                    byte[] buffer = new byte[8192];
                    int len;
                    while ((len = rawIn.read(buffer)) > 0) {
                        fos.write(buffer, 0, len);
                    }
                });
            }

            ObjectStats result = apiInstance.uploadObject(repository, branch, path)
                    .ifNoneMatch("*")
                    .force(force)
                    .content(content)
                    .execute();

            // Transfer to SUCCESS
            session.transfer(flowFile, REL_SUCCESS);
        } catch (ApiException e) {
            System.err.println("Exception when calling ObjectsApi#uploadObject");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());

            getLogger().error("Failed to upload object to branch '{}' and path {} due to {}", new Object[]{branch, path, e.getResponseBody()});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        } catch (IOException ioException) {
            // Handle file I/O errors
            getLogger().error("Error processing FlowFile content due to {}", new Object[]{ioException});
            session.transfer(flowFile, REL_FAILURE);
        } finally {
            // Clean up the temporary file
            if (content != null && content.exists()) {
                content.delete();
            }
    }

    }
}
