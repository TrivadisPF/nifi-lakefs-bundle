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
package org.apache.nifi.lakefs.service.standard;

import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.HttpBasicAuth;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.lakefs.service.api.LakeFSApiClientService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.util.Arrays;
import java.util.List;

/**
 * Implementation of LakeFSApiClientService interface
 * 
 * @see LakeFSApiClientService
 */
@Tags({"lakefs", "versioning"})
@CapabilityDescription("Defines a LakeFS client instance. Currently only HTTP basic authentication is supported.")
public class StandardLakeFSApiClientService extends AbstractControllerService implements LakeFSApiClientService {
    public static final PropertyDescriptor LAKEFS_URL = new PropertyDescriptor
            .Builder()
            .name("lakefs-url")
            .displayName("LakeFS URL")
            .description("The LakeFS endpoint in the form of http://<host>:<port>")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor
            .Builder()
            .name("username")
            .displayName("Username")
            .description("The username for LakeFS.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();     
            
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor
            .Builder()
            .name("password")
            .displayName("Password")
            .description("The Password for LakeFS.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();                

    private static final List<PropertyDescriptor> DESCRIPTORS = Arrays.asList(
        LAKEFS_URL,
        USERNAME,
        PASSWORD
    );

    private ApiClient apiClient = null;

    /**
     * On Enabled reads Private Keys using configured properties
     *
     * @param context Configuration Context with properties
     * @throws InitializationException Thrown when unable to load keys
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        getLogger().info("Creating client with credentials provider");

        apiClient = Configuration.getDefaultApiClient();
        apiClient.setBasePath(context.getProperty(LAKEFS_URL).getValue().concat("/api/v1"));

         // Configure HTTP basic authorization: basic_auth
        HttpBasicAuth basic_auth = (HttpBasicAuth) apiClient.getAuthentication("basic_auth");
        basic_auth.setUsername(context.getProperty(USERNAME).getValue());
        basic_auth.setPassword(context.getProperty(PASSWORD).getValue());
    }

    /**
     * On Disabled clears Private Keys
     */
    @OnDisabled
    public void onDisabled() {
        apiClient = null;
    }

    /**
     * Get Supported Property Descriptors
     *
     * @return Supported Property Descriptors
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    public ApiClient getApiClient() {

        return apiClient;        
    }

}
