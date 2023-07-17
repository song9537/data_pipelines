# data_pipelines

The intent of a pipeline is to collect data from a source and push it to a standardized endpoint, such that
it is accessible for use by models.

NOTES
- dont put any authentication details in here, API keys, etc.

MAKING NEW PIPELINES:
- name the pipeline
  - the name should indicate the source format, and final format of the data, e.g. source_format
  - include a docstring plz
- organize similar functions in a file
- plz update requirements.txt when you add libraries
