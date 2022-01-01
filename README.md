# Jurgen - Data Lake Analytics

============

ETL and analysis are important for data science workflow.
The hardest part of the whole project is to set up EMR cluster.
For example, getting Git repository linked requires setting up VPC
correctly with NAT. At the same time setting up NAT and making VPC
private disallow direct connection to Master note to view UI.

---

## Features
- AWS EMR Studio Support

---

## Setup
Clone this repo:
```
git@github.com:andreiliphd/jurgen-spark-etl.git
```


---

## Notebook
In a file `etl.ipynb` you can find Jupyter Notebook for running in EMR Studio.

---

## Usage
Run EMR Studio and submit steps.
Use the following command to execute Spark job on a cluster.
```shell
aws emr add-steps \
--cluster-id {id_of_a_cluster} \
--steps 'Type=CUSTOM_JAR,Name="Run spark-submit using command-runner.jar",ActionOnFailure=CONTINUE,Jar=command-runner.jar,Args=[spark-submit, {path_to_python_script_in_s3_bucket} ,ArgName1,ArgValue1,ArgName2,ArgValue2]'
```

---

## License
This project is licensed under the terms of the **MIT** license.