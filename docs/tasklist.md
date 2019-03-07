# Project Requirements and Task List
Here are collected all project requirements.

## Object - Database
### Must Have
- [x] Simple connection to any DB of any vendor.
- [x] Connect via credentials
- [x] Connect via the configuration INI file.
- [x] Method to get the list of DB objects.
- [x] Interface to interact with the DB and its objects.

### Could Have
- [x] Specific interface to interact with the DB and its objects.

## Object - Host
### Must Have
- [ ] Simple connection to any remote host by FTP.
- [x] Simple connection to any remote host by SSH.
- [x] Simple connection to any remote host by SFTP.
- [x] Connect with SSH/SFTP via the credentials
- [ ] Connect with SSH/SFTP via the configuration INI file.
- [ ] Connect with FTP via the credentials.
- [ ] Connect with FTP via the configuration INI file.
- [x] Interface to interact with the host by SSH.
- [x] Interface to interact with the host by SFTP.
- [ ] Interface to interact with the host by FTP.

### Could Have
- [x] Specific interface to interact with the remote host.

## Pipeline - One DB Table -> DBLink -> Another DB Table
### Must Have
- [x] Database object with interface.
- [x] Description of DB, table and BDLink.
- [x] Input table as object with methods.
- [x] Mediation table as object with methods.
- [x] Output table as object with methods.
- [x] File log with process details.
- [x] DB table log with most significant process details.
- [x] Logger as object.
- [x] Configurator as object.
- [x] Append extracted data to target table.
- [x] Clear target table from all data.
- [x] Update target table using extracted data.
- [x] Upsert/merge target table using extracted data.
- [x] Extractor as object with only RUN method customized.
- [x] Transformer as object with only RUN method customized.
- [x] Loader as object with only RUN method customized.
- [x] Automatic pipeline objects generation.
- [x] Read the configuration from Python dict or JSON file.
- [x] Method EXTRACT.
- [x] Method TRANSFORM.
- [x] Method LOAD.
- [x] Method PREPARE.
- [x] Method RUN.
- [x] Customize target table structure and parameters.
- [x] Customize target columns parameters.
- [x] Extract data by creation date.
- [x] Restrict data columns.
- [x] Filter data.
- [x] Join data.
- [x] Pipeline ID.
- [x] Pipeline run date.
- [x] Way to define pipeline which loaded certain records.
- [x] Log ID, start and end times, status.
- [x] Log count of records extracted and loaded.
- [x] Alarming.

### Should Have
- [ ] Variables.
- [x] Base classes for extractor, transformer and loader that will allow to replace built-in objects.
- [x] Clean data from full record duplicates.
- [x] Clean data from primary key duplicates.
- [x] Allow or disallow duplicates.
- [x] Error handler for records that was not or should not be loaded.
- [x] Use existing logger instead of self-generated one.
- [x] Run pipeline for past periods.
- [x] Integrate pipeline with job.
- [x] Log subject who run the pipeline (username or job ID).
- [x] Log pipeline run date.
- [x] Log count of error records.
- [x] Log count of updated records.

### Could Have
- [ ] Most popular transformations.
- [x] Put duplicates to error handler (with identity of pipeline).
- [x] Customize all tables names.
- [x] Space trim for strings.
- [ ] Convert to string from other formats.
- [x] Migrate more data for first pipeline launch.
- [x] Oracle parallel execution.
- [x] Oracle compress.
- [ ] Python multithreading.

### Won't Have
- [ ] Resume load from certain stage.
- [ ] Quick undo of pipeline results.
- [ ] Table partitions.

## Pipeline - Host Folder -> SFTP -> Host Another Folder/Another Host Folder
### Must Have
- [x] Host object with interface.
- [x] Description of host, folder and data in it.
- [x] File log with process details.
- [x] Log as object.
- [x] Configurator as object.
- [x] Copy folder files.
- [ ] Move (cut) folder files.
- [ ] Check if file is finished.
- [x] Clear target folder and/or subfolders from all files.
- [x] Input folder as object with methods.
- [x] Mediation folder as object with methods.
- [x] Output folder as object with methods.
- [x] Extractor as object with only RUN method customized.
- [x] Transformer as object with only RUN method customized.
- [x] Loader as object with only RUN method customized.
- [x] Automatic pipeline objects generation
- [x] Read the configuration from Python dict or JSON file.
- [ ] Pipeline ID and run date.
- [ ] Do not stop if a file processing failed.
- [ ] Exclude failed file from target folder if necessary.
- [ ] Log ID, run date.
- [x] Log files list.
- [x] Log files checksum.
- [ ] Log count of files found and loaded.
- [x] Method EXTRACT.
- [x] Method TRANSFORM.
- [x] Method LOAD.
- [x] Method PREPARE.
- [x] Method RUN.
- [x] Alarming.

### Should Have
- [ ] Table log with most significant process details.
- [x] Mask of files that must be loaded.
- [ ] Special logic for subfolders.
- [ ] Zip data.
- [ ] Unzip data.

### Could Have
- [x] Customize folders names.
- [ ] Customize files names.
- [ ] Customize subfolders names.
- [ ] Data archive.
- [ ] Most popular transformations.
- [ ] Validate data.

### Won't Have
- [ ] Resume load from certain stage.
- [ ] Quick undo of pipeline results.
