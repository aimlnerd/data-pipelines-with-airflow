
#variables
```
from airflow.models import Variable
file_uploads = Variable.get(key='file_uploads', deserialize_json=True)
```

#Wasbhook
* Wasbhook.load_file() to upload to blob
* Wasbhook.get_blobs_list() to get list
* Wasbhook.get_file() to download file

# passing data between tasks
https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html
https://www.astronomer.io/guides/airflow-decorators/
https://www.astronomer.io/guides/airflow-passing-data-between-tasks/

# generating dag dynamically
https://airflow.apache.org/docs/apache-airflow/1.10.3/concepts.html?highlight=variable#scope
https://docs.astronomer.io/learn/dynamically-generating-dags#

# airflow concepts
https://airflow.apache.org/docs/apache-airflow/stable/concepts/index.html
