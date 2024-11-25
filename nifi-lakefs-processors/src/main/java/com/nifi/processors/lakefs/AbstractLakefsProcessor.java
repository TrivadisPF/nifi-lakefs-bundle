package com.nifi.processors.lakefs;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.Configuration;
import io.lakefs.clients.sdk.auth.HttpBasicAuth;

public abstract class AbstractLakefsProcessor extends AbstractProcessor {
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

    public static final PropertyDescriptor REPOSITORY = new PropertyDescriptor
            .Builder()
            .name("repository")
            .displayName("Repository")
            .description("The name of the LakeFS repository")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .build();    


    /**
     * Create LakeFS client
     */
    protected ApiClient createClient(final ProcessContext context) {
        getLogger().info("Creating client with credentials provider");

        ApiClient apiClient = Configuration.getDefaultApiClient();
        apiClient.setBasePath(context.getProperty(LAKEFS_URL).getValue().concat("/api/v1"));

         // Configure HTTP basic authorization: basic_auth
        HttpBasicAuth basic_auth = (HttpBasicAuth) apiClient.getAuthentication("basic_auth");
        basic_auth.setUsername(context.getProperty(USERNAME).getValue());
        basic_auth.setPassword(context.getProperty(PASSWORD).getValue());

        return apiClient;
    }

}
