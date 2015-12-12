# TeamMemberChangeMessageScript
A script that create or update change messages for team member objects

# To run
run with arguments: <local/staging/prod> <synapseAdminUsername> <apiKey> <filePath>

# Create input file
Run this command in the prod database

SELECT * FROM GROUP_MEMBERS;

Export the output of this command to an csv file - the filePath parameter points to this file

NOTE: Remember to remove header of the csv file.
