package com.nifi.processors.lakefs;

import io.lakefs.clients.sdk.ApiClient;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lakefs.service.api.LakeFSApiClientService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

public abstract class AbstractLakefsProcessor extends AbstractProcessor {

    public static final PropertyDescriptor LAKEFS_SERVICE = new PropertyDescriptor.Builder()
            .name("lakefs-service")
            .displayName("LakeFS Service")
            .description("LakeFS Service for working with LakeFS")
            .identifiesControllerService(LakeFSApiClientService.class)
            .required(true)
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

    private LakeFSApiClientService getLakeFSApiClientService(final ProcessContext context) {
        LakeFSApiClientService lakeFSApiClientService = null;
        final PropertyValue lakeFSApiCllientServiceProperty = context.getProperty(LAKEFS_SERVICE);
        if (lakeFSApiCllientServiceProperty.isSet()) {
            lakeFSApiClientService = lakeFSApiCllientServiceProperty.asControllerService(LakeFSApiClientService.class);
        }
        return lakeFSApiClientService;
    }

    protected ApiClient getApiClient(final ProcessContext context) {
        ApiClient apiClient = getLakeFSApiClientService(context).getApiClient();
        return apiClient;
    }

}
