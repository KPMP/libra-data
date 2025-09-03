# Changelog

## Release 1.10 [Unreleased]
Breif summary:

### Breaking changes

### Other changes

---

## Release 1.9 [Released 8/2/2025]
Brief summary
- Load Tableau database with biopsy_tracker data
- Insert dlu_upload_type to dlu_package_inventory table
- Create recalled packages endpoint
- Update Multi Modal package name
- Tweak bulk upload fields

### Breaking changes
- changed column names in tables
- added new columns to dlu_package_inventory table

---

## Release 1.8.1 [Released 11/8/2024]
Brief summary of what's in this release:
Fix to the dlu-watcher script to generate a new UUID for each file, rather than one at start up. This caused issues with moving files


### Breaking changes

Breaking changes include any database updates needed, if we need to edit any files on system (like .env or certs, etc). Things that are outside of the code itself that need changed for the system to work.


### Non-breaking changes

Just a place to keep track of things that have changed in the code that we may want to pay special attention to when smoke testing, etc.


---

## Release 1.8 [released 10/16/2024]
Brief summary of what's in this release:

- Added logic to change ownership of moved files off of root user
- Removed logic that required connection to spectrack for dlu-watcher
- updated logic for handling nested folders
- updated base image of DluWatcher container
- set install of requirements as 'progress-banner off' to address issue with installing Flask and using all threads in DluWatcher container

### Breaking changes

Breaking changes include any database updates needed, if we need to edit any files on system (like .env or certs, etc). Things that are outside of the code itself that need changed for the system to work.


### Non-breaking changes

Just a place to keep track of things that have changed in the code that we may want to pay special attention to when smoke testing, etc.
