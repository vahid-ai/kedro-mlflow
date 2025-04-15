Create a kedro pipline with mlflow integration and feast feast feature store to create and evaluate multiple models for the open aws s3 dataset called A Realistic Cyber Defense Dataset (CSE-CIC-IDS2018).
Here are the details for the aws s3 datasest:

Resource type
S3 Bucket
Amazon Resource Name (ARN)

arn:aws:s3:::cse-cic-ids2018
AWS Region
ca-central-1
AWS CLI Access (No AWS account required)

aws s3 ls --no-sign-request s3://cse-cic-ids2018/

Use Ibis with Duckdb as the data store. 
Use feast as a feature store with DuckDB as the backend engine.
Optimize the kedro pipeline to be compatible with Jupyter notebooks.
For data processing utilize Ray Data and Dask for Ray, duckdb and ibis dataframes to run on a local NVIDIA GPU, 
effeciently utilizing CPU and GPU and effeciently handling context switching and pieplining between CPU and GPU at no point is either sitting idle waiting for the other.

For feature engineering use the best methods, libraries and frameworks prefereably AutoML methods when application.
Here are some feature engineering library examples, but feel freee to use others if they are better suited to the current task:
- Featuretools: https://github.com/alteryx/featuretools
- AutoFeat: https://github.com/cod3licious/autofeat
- TSFresh: https://github.com/blue-yonder/tsfresh
- FeatureSelector: https://github.com/WillKoehrsen/feature-selector
- PyCaret
- H2O AutoML
- TPOT
- Auto-sklearn
- FLAML
- EvalML
- Auto-ViML
- AutoGluon

Use ray tune with either Ax, BayesOpt, BOHB, Nevergrad, or Optuna for hyperparameter optimization.
The machine learning models should be a an XGBoost model and an Autoencoder built with pytorch. 
The xgboost model can use an existing library, but the autoencoder should be built from scratch with pytorch, pytorch lightning and ray for pytorch lightning, utilizing and integrating with mlflow.
Implement visualiziation for the entire piepline utilizing kedro visualization and any other methods, libraries, frameworks or code if necessary.
Add metrics for model drift for model inferencing.
Finally, use python and use UV as the python package manager