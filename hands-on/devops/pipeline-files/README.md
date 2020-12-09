# Import CI & CD Pipeline Files

## Import CI Pipeline
Navigate to `pipeline-files` and download the `azure-pipelines.yml` file.

In https://dev.azure.com/, navigate to Repos > Files and click the three dots next to the `root` Repo name. In the dropdown select `Upload File(s)` and select the `azure-pipelines.yml` file. 

Navigate to Pipelines > Pipelines. Click on `New pipeline` > choose `Azure Repo Git` > choose your repo > select `Existing Azure Pipelines YAML file`. In the Path dropdown, select the `azure-pipelines.yml` file you just uploaded. Click continue and the the dropdown next to `Run` and click `Save`. 

## Import CD Release Pipeline
Navigate to `pipeline-files` and download the `Continuous Deployment Release Pipeline.json` file. 

In https://dev.azure.com/, navigate to Pipelines > Releases. Click on `+ New` dropdown and select the `Import Release Pipeline` option. Upload the downloaded pipeline file and click `OK`. 

Some tasks in the tasks of Stage 1 will need some final configurations to get set up. 

Agent job > Agent pool > Select `Default`

Configure Databricks CLI > Workspace URL > enter your Databricks workspace URL

Configure Databricks CLI > Access Token > enter your Databricks Personal Access Token _instructions on how to find this is available in the walkthrough slides available at_ `devops/`

Deploy Notebooks to Workspace > Notebooks folder > click on the three dots to browse the folder system - select the location in the repository where your notebooks are located

Deploy Notebooks to Workspace > Workspace folder > if left blank will default to the `/Shared` folder in your Databricks workspace. 