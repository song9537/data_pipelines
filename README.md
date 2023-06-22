# data_pipelines

The intent of a pipeline is to collect data from a source, and make it accessible for use by
models. A pipeline should typically follow the ETL protocol , if these features are separated
within a file, there should be a function in the same file combining them into a complete
pipeline.

NOTES
- dont put any authentication details in here 
  - use some sort of keyring or gnupg system for maintaining auth details in system
  - using .env acutally right now

MAKING NEW PIPELINES:
- name the pipeline
  - the name should indicate the source format, and final format of the data
  - include a docstring plz
- pipelines of the same type can be in a file of thier own or not 
  - dont really care just dont make a mess
- might be useful to import all completed pipelines to single file for importing purposes
- plz update requirements.txt when you add libraries

POSSIBLE ENDPOINTS:
- end points need to be accessible to multiple machines
- concurrency is not required but ideal

Mongo DB
- concurrent access
- multiple contributors
