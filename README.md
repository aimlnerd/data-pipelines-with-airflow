# Requirements
Ubuntu/Debian OS

Go to http://localhost:8084/ for web interface


# Connections
Install required providers
configure azure blob store connection

  1. Connection id - give any name say azure_default
  2. Connecection type - Azure (from drop down)
  3. Extra - use azurite connection string i.e. 
    {"connection_string": "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://blob-storage:10000/devstoreaccount1;QueueEndpoint=http://blob-storage:10001/devstoreaccount1;TableEndpoint=http://blob-storage:10002/devstoreaccount1;"}
  

# Variable
In the UI add variables that can be shared

# To stop and remove all running dockers
```
docker kill $(docker ps -q)
docker rm $(docker ps -a -q)
```


# Dependency
make sure the container is created in Azure blob store
