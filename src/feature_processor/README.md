## Preparation
 - Supported OS: Ubuntu 22.04
 - Do all the preparation steps in src/ingestion.
 - Install nltk python package ```pip3 install nltk```
 - Install autocorrect python package ```pip3 install autocorrect```

## Feature Processor Programs
 - This program processes raw content and user data and turn them into features usable by the ranking model:
```python3 -m src.feature_processor.main_features_gen --postgres_host="IP of your postgres host" --postgres_user="postgres user name" --postgres_password="postgres password" --output_path="data set output path"```

- This program processes partitions rating datasets into training, testing and validation sets, usable by the ranking model:
```python3 -m src.feature_processor.main_rating_data_set_gen --postgres_host="IP of your postgres host" --postgres_user="postgres user name" --postgres_password="postgres password"  --output="data set output path"```
