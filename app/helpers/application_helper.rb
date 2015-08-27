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
		outPut = Array.new
		outPut.push("Starting Test")
		# local connection info
		host = "bluemix.ibm.com"
		port = "10214"
		
=begin 
		parsing of vcap services for bluemix example
		if ENV['VCAP_SERVICES'] == nil
			outPut.push("vcap services is nil")
			return outPut
		end
		vcap_hash = JSON.parse(ENV['VCAP_SERVICES'])["altadb-dev"]
		credHash = vcap_hash.first["credentials"]
		host = credHash["host"]
		port = credHash["json_port"]
		jsonUrl = credHash["json_url"]
		dbname= credHash['db']
		user = credHash["username"]
		password = credHash["password"]
=end
		
		kansasCity = City.new("Kansas City", 467007, 39.0997, 94.5783, 1)
		seattle = City.new("Seattle", 652405, 47.6097, 122.3331, 1);
		newYork = City.new("New York", 8406000, 40.7127, 74.0059, 1);
		london = City.new("London", 8308000, 51.5072, 0.1275, 44);
		tokyo = City.new("Tokyo", 13350000, 35.6833, -139.6833, 81);
		madrid = City.new("Madrid", 3165000, 40.4000, 3.7167, 34);
		melbourne = City.new("Melbourne", 4087000, -37.8136, -144.9631, 61);
		sydney = City.new("Sydney", 4293000, -33.8650, -151.2094, 61)

		mongo_client = Mongo::Client.new(["#{host}:#{port}"])
		db = mongo_client.database
		collectionName = "rubyMongoGalaxy"
		joinCollectionName = "rubyJoin"
		cityTableName = "citytable"
		codeTableName = "codetable"

		outPut.push("Creating collection #{collectionName}")	
		mongo_client[collectionName].drop # make sure it does not already exist
		collection = mongo_client[collectionName]
		collection.create

		outPut.push("")
		outPut.push("Create tables: #{codeTableName}, #{cityTableName}")
		# drop tables before
		db["codeTable"].drop
		db["cityTable"].drop

		db.command({"create" => "codetable", :columns => [{:name => "countryCode", :type => "int"},
						{:name => "countryName", :type => "varchar(50)"}]})
		db.command({"create" => "citytable", :columns => [{:name => "name", :type => "varchar(50)"},
						{:name => "population", :type => "int"}, {:name => "longitude", :type => "decimal(8,4)"},
						{:name => "latitude", :type => "decimal(8,4)"}, {:name => "countryCode", :type => "int"}]})
		
		outPut.push(" ")
		outPut.push("Insert a single document to a collection")
		collection.insert_one(kansasCity.toHash)
		outPut.push("Inserted #{kansasCity.toHash}" )

		outPut.push(" ")
		outPut.push("Inserting multiple entries into collection")
		multiPost = [seattle.toHash(), newYork.toHash(), london.toHash(), tokyo.toHash(), madrid.toHash()] 
		collection.insert_many(multiPost)
		outPut.push("Inserted")
		outPut.push("#{seattle.toHash}")
		outPut.push("#{newYork.toHash}")
		outPut.push("#{london.toHash}")
		outPut.push("#{tokyo.toHash}")
		outPut.push("#{madrid.toHash}") 

		outPut.push(" ")
		outPut.push("Find one that matches a query condition")
		outPut.push("#{kansasCity.name}")
		outPut.push("#{collection.find({:name => kansasCity.name}).to_a}")

		outPut.push(" ")
		outPut.push("Find all that match a query condition: longitude > 40")
		collection.find("longitude" => {"$gt" => 40}).each do |row|
			outPut.push("#{row.to_a}")
		end

		outPut.push(" ")
		outPut.push("Find all documents in collection")
		collection.find.each do |row|
			outPut.push(row)
		end

		outPut.push("")
		outPut.push("Count documents in collection")
		num = collection.find({:population => {"$lt" => 8000000}}).count()
		outPut.push("There are #{num} documents with a population less than 8 million")

		outPut.push("")
		outPut.push("Order documents in collection by population (high to low)")
		result = collection.find.sort(:population => -1).projection(:name =>1, :population => 1, :_id => 0).each do |row|
			outPut.push(row)
		end

		outPut.push("")
		outPut.push("Find distinct codes in collection")
		collection.find.distinct(:countryCode).each do |row|
			outPut.push(row)	
		end

		outPut.push("")
		outPut.push("Joins")
		
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

		outPut.push("Join collection-collection")
    	joinCollectionCollection = {"$collections" => {"rubyMongoGalaxy" => {"$project" => {:name => 1 ,:population => 1 ,:longitude => 1 ,:latitude => 1}} , 
                                                   "rubyJoin" => { "$project" => {:countryCode => 1 ,:countryName =>1}}} , 
                                "$condition" => {"rubyMongoGalaxy.countryCode" => "rubyJoin.countryCode"}}

		outPut.push("Find all documents in join collection")
		sys.find(joinCollectionCollection).each do |row|
			outPut.push(row)
		end

		outPut.push("")
		outPut.push("Join table-collection")
		joinTableCollection = {"$collections" => {"citytable" => {"$project" => {:name => 1, :population => 1, :longitude => 1, :latitude => 1}},
								"rubyJoin" => {"$project" => {:countryCode => 1, :countryName => 1}}},
								"$condition" => {"citytable.countryCode" => "rubyJoin.countryCode"}}
		outPut.push("Find all documents table-collection")
		sys.find(joinTableCollection).each do |row|
			outPut.push(row)
		end

		outPut.push("Join table-table")
		joinTableTable = {"$collections" => {"citytable" => {"$project" => {:name => 1, :population => 1, :longitude => 1, :latitude => 1}},
							"codetable" => {"$project" => {:countryCode => 1, :countryName => 1}}},
							"$condition" => {"citytable.countryCode" => "codetable.countryCode"}}
		outPut.push("Find all documents in table-table")
		sys.find(joinTableTable).each do |row|
			outPut.push(row)
		end

		
		outPut.push("Projection: Display results without longitude and latitude")
		# projection is a method of Module: Mongo::Collection::View::Readable
		collection.find({:countryCode => 1}).projection({:_id => 0, :longitude => 0, :latitude =>0}).each do |rst|
			outPut.push(rst)
		end

		outPut.push("")
		outPut.push("Update Documents")
		collection.find(:name => seattle.name).update_one("$set" => {:countryCode => 999})
		outPut.push("Updated #{seattle.name} with countryCode 999")

		outPut.push("")
		outPut.push("Delete Documents")
		result = collection.find(:name => tokyo.name).delete_many
		unless result != 1
			outPut.push("Failed to delete only document with #{tokyo.name}")
		end
=begin 
		collection_names returns The regular expression /system\.|\$/ is not supported. 
		This was tested with Linux 64 server and Windows 64 client
			outPut.push("")
			outPut.push("Get a list of all of the collections")
			result = db.collection_names
			outPut.push(result)
=end

		outPut.push("")
		outPut.push("SQL passthrough")
		# You must enable SQL operations by setting security.sql.passthrough=true in the wire listener properties file.
		# remove table if already exist
		sqlTable = mongo_client["town"].drop
		# the table needs to be created through database, not mongo client for passthrough
		sqlCollection = db["system.sql"]
		outPut.push("Create table")
		result = sqlCollection.find("$sql" => "create table town (name varchar(255), countryCode int)")
		outPut.push("Insert into table")
		result = sqlCollection.find("$sql" => "insert into town values ('Lawerence', 1)")
		outPut.push("Drop table")
		result = sqlCollection.find("$sql" => "drop table town")

		#Transactions
		outPut.push("Transactions")
		db.command(:transaction => "enable")
		collection.insert_one(sydney.toHash)
		db.command(:transaction => "commit")
		collection.insert_one(tokyo.toHash)
		collection.insert_one(melbourne.toHash)
		db.command(:transaction => "rollback")
		db.command(:transaction => "disable")

		outPut.push(" ")
		outPut.push("List of all documents in collection")
		result = collection.find.projection(:name =>1, :population => 1, :_id => 0).each do |row|
			outPut.push(row)
		end

		outPut.push("")
		outPut.push("Commands")
		count = db.command({"count" => "#{collectionName}"})
		count.each do |stmt|
			outPut.push("There are #{stmt['n']} documents in collection}")
		end
		
		rst = db.command({"distinct" => "#{collectionName}", "key" => "countryCode"})
		rst.each do |stmt|
			outPut.push("Distinct values: #{stmt['values']}")
		end
		# Database stats
		rst = db.command({"dbstats" => 1})
		#outPut.push[0]
		rst.each do |stmt|
			outPut.push(stmt)
		end
		# collection stats
		rst = db.command({"collstats" => "#{collectionName}"})
		rst.each do |stmt|
			outPut.push(stmt)
		end
		outPut.push("")
		outPut.push("Drop a collection")
		collection.drop
		outPut.push("Drop database")
		db.drop
		

		return outPut
	end
end

