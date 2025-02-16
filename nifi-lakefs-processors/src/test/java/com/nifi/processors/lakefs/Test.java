package com.nifi.processors.lakefs;

import io.lakefs.clients.sdk.*;
import io.lakefs.clients.sdk.auth.HttpBasicAuth;
import io.lakefs.clients.sdk.model.*;

public class Test {

    protected static String getCommitId(ApiClient apiClient, String repository, String branchName) throws ApiException {
        String commitId = null;
        BranchesApi apiInstance = new BranchesApi(apiClient);
        try {
            Ref result = apiInstance.getBranch(repository, branchName).execute();
            commitId = result.getCommitId();
        } catch (ApiException ex) {
            if (ex.getResponseBody().contains("branch not found")) {
                System.out.println("Branch " + branchName + " does not exist!");
            } else {
                throw ex;
            }
        }
        return commitId;
    }

    public static RefList listBranches(ApiClient apiClient, String repository) throws ApiException {
        BranchesApi apiInstance = new BranchesApi(apiClient);
        RefList result = apiInstance.listBranches(repository).execute();
        return result;
    }

    public static Commit getCommit(ApiClient apiClient, String repository, String commitId) throws ApiException {
        CommitsApi apiInstance = new CommitsApi(apiClient);
        Commit result = apiInstance.getCommit(repository, commitId).execute();
        return result;
    }

    public static StorageURI createSymlinkFile(ApiClient apiClient, String repository, String branchName, String location) throws ApiException {
        InternalApi apiInstance = new InternalApi(apiClient);

        StorageURI result = apiInstance.createSymlinkFile(repository, branchName)
                .location(location)
                .execute();
        return result;
    }

    public static String createBranch(ApiClient apiClient, String repository, String branchName, String source, boolean force, boolean hidden, boolean ignore) throws ApiException {
        String result = null;
        BranchesApi apiInstance = new BranchesApi(apiClient);
        try {
            BranchCreation branchCreation = new BranchCreation();
            branchCreation.setSource(source);
            branchCreation.setName(branchName);
            branchCreation.setForce(force);
            branchCreation.setHidden(hidden);

            result = apiInstance.createBranch(repository, branchCreation).execute();
        } catch (ApiException ex) {
            if (ignore && ex.getResponseBody().contains("branch already exists")) {
                System.out.println("Branch " + branchName + " already exists, ignoring it!");
            } else {
                throw ex;
            }
        }
        return result;
    }



    public static void main(String[] args) throws ApiException {
        ApiClient apiClient = Configuration.getDefaultApiClient();
        apiClient.setBasePath("http://localhost:28220".concat("/api/v1"));

        // Configure HTTP basic authorization: basic_auth
        HttpBasicAuth basic_auth = (HttpBasicAuth) apiClient.getAuthentication("basic_auth");
        basic_auth.setUsername("admin");
        basic_auth.setPassword("platysEADP2024!");

        System.out.println(createBranch(apiClient, "demo", "hidden", "main", false, false, false));


        RefList list = listBranches(apiClient, "demo");
        System.out.println(list);
        System.out.println(list.getResults().getFirst().toJson());

        String commitId = getCommitId(apiClient, "demo", "example-branch");
        if (commitId != null) {
            Commit commit = getCommit(apiClient, "demo", commitId);

            System.out.println(commit.getParents().toString());
        }

        StorageURI s = createSymlinkFile(apiClient, "demo", "example-branch", "path/to/_SUCCESS");
        System.out.println(s);
    }
}
