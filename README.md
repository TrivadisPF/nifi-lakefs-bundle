<p align="center">
  <img src="https://raw.githubusercontent.com/treeverse/lakeFS/master/docs/assets/img/logo_large.png"/>
</p>

## lakeFS Apache NiFi Processors and Services

LakeFS Nifi processors & services enables a smooth integration of lakeFS with Apache NiFi's data flows. "Use the lakeFS NiFi bundle to create branches, commit objects, wait for files to be written, and more."

For usage example, check out the [example NiFi data flow](to do).

## What is lakeFS

lakeFS is an open source layer that delivers resilience and manageability to object-storage based data lakes.

With lakeFS you can build repeatable, atomic and versioned data lake operations - from complex ETL jobs to data science and analytics.

lakeFS supports AWS S3, Azure Blob Storage and Google Cloud Storage as its underlying storage service. It is API compatible with S3, and works seamlessly with all modern data frameworks such as Spark, Hive, AWS Athena, Presto, etc.

For more information see the [official lakeFS documentation](https://docs.lakefs.io).

## Capabilities

### Commits API

| LakeFS API | Descritption | NiFI Processor | Status |
|------------|:-------:	|-------	|---- |
| `commit`	| commit to a branch | `CommitLakeFS` | implemented
| `getCommit` | fetch commit details for a given commit id | `FetchCommitLakeFS` <br>(gets details for a given commit id into NiFi attributes) | implemented | 
| `getBranch`+`getCommit` | gets the latest commit id and it's details | `WaitCommitLakeFS` <br>(waits for a commit to happen on the branch and transfers to the the success relationship | implemented |
 
### Branches API

| LakeFS API | Description | NiFI Processor |Status |
|------------|:-------:	|-------	|---- |
| `listBranches `	| list branches | `ListBranchLakeFS` <br>(similar to ListFile - returns first all branches and then only new ones) | implemented |
| `createBranch ` | create branch | `CreateBranchLakeFS` | implemented |
| `cherryPick ` | cherry-pick a ref | n.a. | |
| `deleteBranch ` | deletes a branch  | `DeleteBranchLakeFS` | implemented |
| `diffBranch ` | get diff for a branch | n.a. | |
| `getBranch ` | get branch details| `FetchBranchLakeFS`<br>(returns branch details as NiFi attributes) | implemented |
| `resetBranch ` | reset changes on a branch | `ResetBranchLakeFS` | |
| `revertBranch ` | revert commit for branch | `RevertBranchLakeFS` | |

### Refs API

| LakeFS API | Descritption | NiFI Processor |Status |
|------------|:-------:	|-------	|---- |
| `diffRefs` | diff two refs | n.a. | |
| `log_commits` | get commit log from ref | `FetchLogCommitsLakeFS` | |
| `mergeIntoBranch` | merge references  | `MergeLakeFS` | implemented |

### Pull Request API

| LakeFS API | Description | NiFI Processor |Status |
|------------|:-------:	|-------	|---- |
| `createPullRequest` | creates a pull request | `CreatePullRequestLakeFS` | implemented |
| `mergePullRequest` | merges a pull request | `MergePullRequestLakeFS` | implemented |
| `getPullRequest` | get details for a pull reqeust | `FetchPullRequestLakeFS` | |
| `updatePullRequest` | update pull reqeust | n.a. | |
| `listPullRequests` | list pull requests | `ListPullRequestLakeFS` | implemented |

### Objects API

| LakeFS API | Descritption | NiFI Processor |Status |
|------------|:-------:	|-------	|---- |
| `copyObject`	| create a copy of an object | `CopyObjectLakeFS` | |
| `deleteObject`	| delete an object  given a path relative to branch/ref | `DeleteObjectLakeFS` | |
| `deleteObjects`	| delete objects given a list of paths relative to branch/ref | `DeleteObjectLakeFS` | |
| `getObject`	| get an object at a path relative to branch/ref | `FetchObjectLakeFS` | implemented |
| `getUnderlyingProperties`	| get an object's storage class properties at a path relative to branch/ref. |n.a. | |
| `headObject`	| get object's existence at a path relative to branch/ref | n.a. | |
| `listObjects`	| list objects and its metadata at given ref prefix | `ListObjectLakeFS`<br> (similar to ListFile) | |
| `statObject	`| fetch object's metadata at given ref prefix | n.a. | |
| `uploadObject`	| upload object at given path for ref prefix | `PutObjectLakeFS` | implemented |

### Internal API

| LakeFS API | Descritption | NiFI Processor |Status |
|------------|:-------:	|-------	|---- |
| `createSymlinkFile`	| creates symlink files corresponding to the given directory | `CreateSymlinkFileLakeFS` | implemented |

