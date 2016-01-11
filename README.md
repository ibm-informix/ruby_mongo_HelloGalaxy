# Ruby on Rails Mongo Hello Galaxy Sample Application for Informix 

## Where are the important files?

### app/helpers/application_helper.rb 

All of the Ruby code for accessing and interacting with the IBM Informix database server using the Ruby Mongo driver is in the application_helper.rb file.

## What can I do with this example?

### Option 1: Deploy to Bluemix

#### Requirements:

Git - Used to download the application.

CloudFoundry CLI -  Used to push the application to Bluemix.

#### Procedure:

 * Step 1: Clone repository to local machine
	
 * Step 2: Push application to Bluemix using CloudFoundry CLI.
 
### Option 2: Run the app locally

#### Requirements:

Git - Used to download the application.

A locally accessible instance of the IBM Informix database server and the Informix JSON listener

#### Procedure: 

* Step 1: Install Ruby 2.2

* Step 2: Clone the repository to a local machine

* Step 3: cd into the app directory

* Step 4: Edit the file app/helpers/application_helper.rb, to set the URL to your Informix JSON listener 

* Step 5: Run `gem install bundler` to install bundler

* Step 6: Run `bundler install` to install app dependencies

* Step 7: Run `rails server`

* Step 8: Access the running app in a browser at http://localhost:3000
