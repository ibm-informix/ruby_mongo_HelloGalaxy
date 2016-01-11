module ApplicationHelper
##
# Ruby Sample Application: Connection to Informix using Mongo
##

# Documentation for ruby mongo api used is https://api.mongodb.org/ruby/current/Mongo.html

# Topics
# 1 Data Structures
# 1.1 Create collection
# 1.2 Create table
# 2 Inserts
# 2.1 Insert a single document into a collection 
# 2.2 Insert multiple documents into a collection 
# 3 Queries
# 3.1 Find one document in a collection 
# 3.2 Find documents in a collection 
# 3.3 Find all documents in a collection 
# 3.4 Count documents in a collection 
# 3.5 Order documents in a collection 
# 3.6 Find distinct fields in a collection 
# 3.7 Joins
# 3.7a Collection-Collection join
# 3.7b Table-Collection join
# 3.7c Table-Table join 
# 3.8 Modifying batch size 
# 3.9 Find with projection clause 
# 4 Update documents in a collection 
# 5 Delete documents in a collection 
# 6 SQL passthrough 
# 7 Transactions
# 8 Commands
# 8.1 Count  
# 8.2 Distinct 
# 8.3 CollStats 
# 8.4 DBStats 
# 9 Drop a collection

	# To run locally, set URL here.
	# For example, URL = "mongodb://localhost:27017/test"	
	URL = ""
	
	# When deploying to Bluemix, determines whether to use SSL
	USE_SSL = false
	
	class City
		attr_accessor :name, :population, :longitude, :latitude, :countryCode

		def initialize(name, population, latitude, longitude, countryCode)
			@name = name
			@population = population
			@longitude = longitude
			@latitude = latitude
			@countryCode = countryCode
		end
	    
	    def toHash
	    	return {:name => @name, :population => @population, :latitude => @latitude, :longitude => @longitude, :countryCode => @countryCode}
	    end
	end

	def runHelloGalaxy()
		# create array to store info for output
		output = Array.new
		output.push("Starting test...")

		if (URL == nil || URL == "") 
			logger.info("parsing VCAP_SERVICES")
			if ENV['VCAP_SERVICES'] == nil
				output.push("vcap services is nil")
				return output
			end
                        serviceName = "timeseriesdatabase"
			if ENV['SERVICE_NAME'] != nil
				serviceName = ENV['SERVICE_NAME']
			end
			logger.info("Using service name " + serviceName)
			vcap_hash = JSON.parse(ENV['VCAP_SERVICES'])[serviceName]
			credHash = vcap_hash.first["credentials"]
			if (USE_SSL)
				mongodb_url = credHash["mongodb_url_ssl"]
			else
				mongodb_url = credHash["mongodb_url"]
			end
			logger.info("Using mongodb_url " + mongodb_url)
		else
			mongodb_url = URL
		end
		
		kansasCity = City.new("Kansas City", 467007, 39.0997, 94.5783, 1)
		seattle = City.new("Seattle", 652405, 47.6097, 122.3331, 1);
		newYork = City.new("New York", 8406000, 40.7127, 74.0059, 1);
		london = City.new("London", 8308000, 51.5072, 0.1275, 44);
		tokyo = City.new("Tokyo", 13350000, 35.6833, -139.6833, 81);
		madrid = City.new("Madrid", 3165000, 40.4000, 3.7167, 34);
		melbourne = City.new("Melbourne", 4087000, -37.8136, -144.9631, 61);
		sydney = City.new("Sydney", 4293000, -33.8650, -151.2094, 61)

		begin
			output.push("Connecting to " + mongodb_url)
			mongo_client = Mongo::Client.new(mongodb_url)
			db = mongo_client.database
			collectionName = "rubyMongoGalaxy"
			joinCollectionName = "rubyJoin"
			cityTableName = "citytable"
			codeTableName = "codetable"
	
			output.push("Creating collection #{collectionName}")	
			mongo_client[collectionName].drop # make sure it does not already exist
			collection = mongo_client[collectionName]
			collection.create
	
			output.push("")
			output.push("Create tables: #{codeTableName}, #{cityTableName}")
			# drop tables before
			db["codeTable"].drop
			db["cityTable"].drop
	
			db.command({"create" => "codetable", :columns => [{:name => "countryCode", :type => "int"},
							{:name => "countryName", :type => "varchar(50)"}]})
			db.command({"create" => "citytable", :columns => [{:name => "name", :type => "varchar(50)"},
							{:name => "population", :type => "int"}, {:name => "longitude", :type => "decimal(8,4)"},
							{:name => "latitude", :type => "decimal(8,4)"}, {:name => "countryCode", :type => "int"}]})
			
			output.push(" ")
			output.push("Insert a single document to a collection")
			collection.insert_one(kansasCity.toHash)
			output.push("Inserted #{kansasCity.toHash}" )
	
			output.push(" ")
			output.push("Inserting multiple entries into collection")
			multiPost = [seattle.toHash(), newYork.toHash(), london.toHash(), tokyo.toHash(), madrid.toHash()] 
			collection.insert_many(multiPost)
			output.push("Inserted")
			output.push("#{seattle.toHash}")
			output.push("#{newYork.toHash}")
			output.push("#{london.toHash}")
			output.push("#{tokyo.toHash}")
			output.push("#{madrid.toHash}") 
	
			output.push(" ")
			output.push("Find one that matches a query condition")
			output.push("#{kansasCity.name}")
			output.push("#{collection.find({:name => kansasCity.name}).to_a}")
	
			output.push(" ")
			output.push("Find all that match a query condition: longitude > 40")
			collection.find("longitude" => {"$gt" => 40}).each do |row|
				output.push("#{row.to_a}")
			end
	
			output.push(" ")
			output.push("Find all documents in collection")
			collection.find.each do |row|
				output.push(row)
			end
	
			output.push("")
			output.push("Count documents in collection")
			num = collection.find({:population => {"$lt" => 8000000}}).count()
			output.push("There are #{num} documents with a population less than 8 million")
	
			output.push("")
			output.push("Order documents in collection by population (high to low)")
			result = collection.find.sort(:population => -1).projection(:name =>1, :population => 1, :_id => 0).each do |row|
				output.push(row)
			end
	
			output.push("")
			output.push("Find distinct codes in collection")
			collection.find.distinct(:countryCode).each do |row|
				output.push(row)	
			end
	
			output.push("")
			output.push("Joins")
			
			# refer to documentation for system.join operability
			# http://www-01.ibm.com/support/knowledgecenter/SSGU8G_12.1.0/com.ibm.json.doc/ids_json_069.htm?lang=en
			sys = db["system.join"]
			
			mongo_client[joinCollectionName].drop # make sure it does not already exist
			joinCollection = mongo_client[joinCollectionName]
			joinCollection.create
			joinCollection.insert_one({:countryCode => 1, :countryName => "United States of America"})
			joinCollection.insert_one({:countryCode => 44, :countryName => "United Kingdom"})
			joinCollection.insert_one({:countryCode => 81, :countryName => "Japan"})
			joinCollection.insert_one({:countryCode => 34, :countryName => "Spain"})
			joinCollection.insert_one({:countryCode => 61, :countryName => "Australia"})
	
			codeTable = db[codeTableName]
			codeTable.insert_one({:countryCode => 1, :countryName => "United States of America"})
			codeTable.insert_one({:countryCode => 44, :countryName => "United Kingdom"})
			codeTable.insert_one({:countryCode => 81, :countryName => "Japan"})
			codeTable.insert_one({:countryCode => 34, :countryName => "Spain"})
			codeTable.insert_one({:countryCode => 61, :countryName => "Australia"})
	
			
			cityTable = db[cityTableName]
			cityTable.insert_one(kansasCity.toHash)
			cityTable.insert_many(multiPost)
	
			output.push("Join collection-collection")
		    	joinCollectionCollection = {"$collections" => {"rubyMongoGalaxy" => {"$project" => {:name => 1 ,:population => 1 ,:longitude => 1 ,:latitude => 1}} , 
				"rubyJoin" => { "$project" => {:countryCode => 1 ,:countryName =>1}}} , 
				"$condition" => {"rubyMongoGalaxy.countryCode" => "rubyJoin.countryCode"}}

			output.push("Find all documents in collection-to-collection join")
			sys.find(joinCollectionCollection).each do |row|
				output.push(row)
			end
	
			output.push("")
			output.push("Join table-collection")
			joinTableCollection = {"$collections" => {"citytable" => {"$project" => {:name => 1, :population => 1, :longitude => 1, :latitude => 1}},
									"rubyJoin" => {"$project" => {:countryCode => 1, :countryName => 1}}},
									"$condition" => {"citytable.countryCode" => "rubyJoin.countryCode"}}
			output.push("Find all documents table-to-collection join")
			sys.find(joinTableCollection).each do |row|
				output.push(row)
			end
	
			output.push("Join table-table")
			joinTableTable = {"$collections" => {"citytable" => {"$project" => {:name => 1, :population => 1, :longitude => 1, :latitude => 1}},
								"codetable" => {"$project" => {:countryCode => 1, :countryName => 1}}},
								"$condition" => {"citytable.countryCode" => "codetable.countryCode"}}
			output.push("Find all documents in table-to-table join")
			sys.find(joinTableTable).each do |row|
				output.push(row)
			end
	
			
			output.push("Projection: Display results without longitude and latitude")
			# projection is a method of Module: Mongo::Collection::View::Readable
			collection.find({:countryCode => 1}).projection({:_id => 0, :longitude => 0, :latitude =>0}).each do |rst|
				output.push(rst)
			end
	
			output.push("")
			output.push("Update Documents")
			collection.find(:name => seattle.name).update_one("$set" => {:countryCode => 999})
			output.push("Updated #{seattle.name} with countryCode 999")
	
			output.push("")
			output.push("Delete Documents")
			result = collection.find(:name => tokyo.name).delete_many
			unless result != 1
				output.push("Failed to delete only document with #{tokyo.name}")
			end
	
			output.push("")
			output.push("SQL passthrough")
			# You must enable SQL operations by setting security.sql.passthrough=true in the wire listener properties file.
			# remove table if already exist
			sqlTable = mongo_client["town"].drop
			# the table needs to be created through database, not mongo client for passthrough
			sqlCollection = db["system.sql"]
			output.push("Create table")
			result = sqlCollection.find("$sql" => "create table town (name varchar(255), countryCode int)")
			output.push("Insert into table")
			result = sqlCollection.find("$sql" => "insert into town values ('Lawerence', 1)")
			output.push("Drop table")
			result = sqlCollection.find("$sql" => "drop table town")
	
			#Transactions
			output.push("")
			output.push("Transactions")
			db.command(:transaction => "enable")
			collection.insert_one(sydney.toHash)
			db.command(:transaction => "commit")
			collection.insert_one(tokyo.toHash)
			collection.insert_one(melbourne.toHash)
			db.command(:transaction => "rollback")
			db.command(:transaction => "disable")
	
			output.push(" ")
			output.push("List of all documents in collection")
			result = collection.find.projection(:name =>1, :population => 1, :_id => 0).each do |row|
				output.push(row)
			end
	
			output.push("")
			output.push("Commands")
			count = db.command({"count" => "#{collectionName}"})
			count.each do |stmt|
				output.push("There are #{stmt['n']} documents in collection")
			end
			
			rst = db.command({"distinct" => "#{collectionName}", "key" => "countryCode"})
			rst.each do |stmt|
				output.push("Distinct values: #{stmt['values']}")
			end
			# Database stats
			rst = db.command({"dbstats" => 1})
			#output.push[0]
			rst.each do |stmt|
				output.push(stmt)
			end
			# collection stats
			rst = db.command({"collstats" => "#{collectionName}"})
			rst.each do |stmt|
				output.push(stmt)
			end
			output.push("")
			output.push("Drop a collection")
			collection.drop
		
		ensure
			if (mongo_client != nil) 
				mongo_client.close
			end	
		end		

		return output

	end
end

